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

type partJob struct {
	multi   *Multi
	part    *Part
	section *io.SectionReader
	size    int64
	md5b64  string
	md5hex  string
}
type partResult struct {
	part *Part
	err  error
}

type UploadWorkerFn func(id int, jobs <-chan partJob, results chan<- partResult)

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

	fmt.Printf("p %#v\n", p)

	m, err := p.b.Multi(p.Key, p.ContentType, p.Acl)
	if err != nil {
		return err
	}

	existing, err := m.ListParts()
	if err != nil && !hasCode(err, "NoSuchUpload") {
		return err
	}

	current := 1

	partc := make(chan partJob, 100)
	resultc := make(chan partResult, 100)

	for w := 0; w < p.WorkerCount; w++ {
		go MultiPartUploader(w, partc, resultc)
	}

	for offset := int64(0); offset < p.TotalSize; offset += p.PartSize {
		part := findMultiPart(existing, current)
		current++

		partc <- partJob{
			multi:   m,
			part:    &part,
			section: io.NewSectionReader(p.ReaderAt, offset, p.PartSize),
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

func MultiPartUploader(id int, jobs <-chan partJob, results chan<- partResult) {
	for job := range jobs {
		result := partResult{part: job.part}

		if job.shouldUpload() {
			result.err = job.put()
		}

		results <- result
	}
}

// idempotently calculate the various things we need to know about this job
func (job *partJob) calculate() error {
	if job.md5hex != "" {
		return nil
	}

	_, err := job.section.Seek(0, 0)
	if err != nil {
		return err
	}
	digest := md5.New()
	job.size, err = io.Copy(digest, job.section)
	if err != nil {
		return err
	}

	sum := digest.Sum(nil)
	job.md5hex = hex.EncodeToString(sum)
	job.md5b64 = base64.StdEncoding.EncodeToString(sum)

	return nil
}

// does this job need uploading?
func (job *partJob) shouldUpload() bool {
	job.calculate()
	return job.md5hex != `"`+job.part.ETag+`"`
}

// do the upload
func (job *partJob) put() error {
	m := job.multi
	job.calculate()

	headers := map[string][]string{
		"Content-Length": {strconv.FormatInt(job.size, 10)},
		"Content-MD5":    {job.md5b64},
	}
	params := map[string][]string{
		"uploadId":   {m.UploadId},
		"partNumber": {strconv.FormatInt(int64(job.part.N), 10)},
	}

	for attempt := attempts.Start(); attempt.Next(); {
		_, err := job.section.Seek(0, 0)
		if err != nil {
			return err
		}

		req := &request{
			method:  "PUT",
			bucket:  m.Bucket.Name,
			path:    m.Key,
			headers: headers,
			params:  params,
			payload: job.section,
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

		job.part.ETag = etag
		job.part.Size = job.size

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
