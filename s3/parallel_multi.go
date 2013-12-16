package s3

import (
	"fmt"
	"io"

	"crypto/md5"
	"encoding/base64"
	"encoding/hex"
	"errors"
	"strconv"
)

var (
	Megabyte                int64 = 1024 * 1024
	MinPartSize             int64 = 5 * Megabyte
	DefaultMultiWorkerCount       = 1
)

type PartJob struct {
	multi   *Multi
	Part    *Part
	Section *io.SectionReader
	Size    int64
	Md5b64  string
	Md5hex  string
}
type partResult struct {
	part *Part
	err  error
}

type UploadWorkerFn func(id int, job PartJob) error

type ParallelUploader struct {
	b            *Bucket
	Key          string
	ReaderAt     io.ReaderAt
	WorkerCount  int
	TotalSize    int64
	PartSize     int64
	ContentType  string
	Acl          ACL
	UploadWorker UploadWorkerFn
}

func (p *ParallelUploader) ValidateAndSetDefaults() error {
	if p.PartSize == 0 {
		p.PartSize = MinPartSize
	}

	if p.PartSize < MinPartSize {
		return fmt.Errorf("PartSize (%s B) is too small (must be at least 5Mb)\n", p.PartSize)
	}

	if p.TotalSize <= 0 {
		return fmt.Errorf("TotalSize (%s B) wasn't set\n", p.TotalSize)
	}

	if p.UploadWorker == nil {
		p.UploadWorker = MultiPartUploader
	}

	if p.WorkerCount == 0 {
		p.WorkerCount = DefaultMultiWorkerCount
	}

	if p.Acl == ACL("") {
		p.Acl = Private
	}

	if p.ContentType == "" {
		p.ContentType = "application/octet-stream"
	}

	return nil
}

//func (b *Bucket) NewParallelUploaderFromFile(key, path string) (*ParallelUploader,error) {
//}

func (b *Bucket) NewParallelUploaderFromReaderAt(key string, r io.ReaderAt, totalSize int64) (*ParallelUploader, error) {
	p := &ParallelUploader{
		b:         b,
		Key:       key,
		ReaderAt:  r,
		TotalSize: totalSize,
	}
	if err := p.ValidateAndSetDefaults(); err != nil {
		return p, err
	}

	return p, nil
}

//func (b *Bucket) NewParallelUploaderFromReaderAtSeeker(key string, r ReaderAtSeeker) (*ParallelUploader,error) {
//}

// func (bucket *Bucket) PutParallel(key string, r io.ReaderAt, workerCount int, totalSize int64, partSize int64, contentType string, acl ACL) error {
func (p *ParallelUploader) Put() error {
	if err := p.ValidateAndSetDefaults(); err != nil {
		return err
	}

	m, err := p.b.Multi(p.Key, p.ContentType, p.Acl)
	if err != nil {
		return err
	}

	existing, err := m.ListParts()
	if err != nil && !hasCode(err, "NoSuchUpload") {
		return err
	}

	current := 1

	partc := make(chan PartJob, 100)
	resultc := make(chan partResult, 100)

	for w := 0; w < p.WorkerCount; w++ {
		go func() {
      for job := range partc {
        resultc <- partResult{
          part: job.Part,
          err: p.UploadWorker(w, job),
        }
      }
    }()
	}

	for offset := int64(0); offset < p.TotalSize; offset += p.PartSize {
		part := findMultiPart(existing, current)
		current++

		partc <- PartJob{
			multi:   m,
			Part:    &part,
			Section: io.NewSectionReader(p.ReaderAt, offset, p.PartSize),
		}
	}
	close(partc)


	// gather results
	partLen := current - 1
	uploadedParts := make([]Part, partLen)
	for i := 0; i < partLen; i++ {
		result := <-resultc
		// XXX fail fast?
		if result.err != nil {
			// add extra info
			return result.err
		}

		uploadedParts[result.part.N-1] = *result.part
	}

	return m.Complete(uploadedParts)
}

func MultiPartUploader(id int, job PartJob) error {
  fmt.Printf("id %d job %#v\n", id, job)
  if job.shouldUpload() {
    return job.put()
  }

  return nil
}


// idempotently calculate the various things we need to know about this job
func (job *PartJob) Calculate() error {
	if job.Md5hex != "" {
		return nil
	}

	_, err := job.Section.Seek(0, 0)
	if err != nil {
		return err
	}
	digest := md5.New()
	job.Size, err = io.Copy(digest, job.Section)
	if err != nil {
		return err
	}

	sum := digest.Sum(nil)
	job.Md5hex = hex.EncodeToString(sum)
	job.Md5b64 = base64.StdEncoding.EncodeToString(sum)

	return nil
}

// does this job need uploading?
func (job *PartJob) shouldUpload() bool {
	job.Calculate()
  fmt.Printf("shouldup %s ?= %s\n", job.Md5b64, job.Part.ETag)
	return job.Md5hex != `"`+job.Part.ETag+`"`
}

// do the upload
func (job *PartJob) put() error {
	m := job.multi
	job.Calculate()

	headers := map[string][]string{
		"Content-Length": {strconv.FormatInt(job.Size, 10)},
		"Content-MD5":    {job.Md5b64},
	}
	params := map[string][]string{
		"uploadId":   {m.UploadId},
		"partNumber": {strconv.FormatInt(int64(job.Part.N), 10)},
	}

	for attempt := attempts.Start(); attempt.Next(); {
		_, err := job.Section.Seek(0, 0)
		if err != nil {
			return err
		}

		req := &request{
			method:  "PUT",
			bucket:  m.Bucket.Name,
			path:    m.Key,
			headers: headers,
			params:  params,
			payload: job.Section,
		}

		err = m.Bucket.S3.prepare(req)
		if err != nil {
			return err
		}

		resp, err := m.Bucket.S3.run(req, nil)
		if shouldRetry(err) && attempt.HasNext() {
			continue
		}
		if err != nil {
			return err
		}

		etag := resp.Header.Get("ETag")
		if etag == "" {
			return errors.New("part upload succeeded with no ETag")
		}

		job.Part.ETag = etag
		job.Part.Size = job.Size

		return nil
	}
	panic("unreachable")
}

func findMultiPart(parts []Part, current int) Part {
	for _, part := range parts {
		if part.N == current {
			return part
		}
	}

	return Part{
		N: current,
	}
}
