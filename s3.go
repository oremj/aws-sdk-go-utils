package awsutils

import (
	"github.com/awslabs/aws-sdk-go/aws"
	"github.com/awslabs/aws-sdk-go/service/s3"
)

// for testing
type objectLister interface {
	ListObjects(input *s3.ListObjectsInput) (*s3.ListObjectsOutput, error)
}

// BucketLister is a helper for listing buckets
type BucketLister struct {
	s3        objectLister
	err       error
	curInput  *s3.ListObjectsInput
	curOutput *s3.ListObjectsOutput
	marker    *string
}

// NewBucketLister returns a new bucket lister
func NewBucketLister(s3Svc *s3.S3, input *s3.ListObjectsInput) *BucketLister {
	return &BucketLister{
		s3:       s3Svc,
		curInput: input,
	}
}

// Err returns any errors that occured
func (b *BucketLister) Err() error {
	return b.err
}

// Next returns true if there are more results
func (b *BucketLister) Next() bool {
	if b.curInput == nil {
		return false
	}

	b.curOutput, b.err = b.s3.ListObjects(b.curInput)
	if b.err != nil {
		return false
	}

	if *b.curOutput.IsTruncated {
		var marker string
		if b.curOutput.NextMarker != nil {
			marker = *b.curOutput.NextMarker
		} else {
			marker = *b.curOutput.Contents[len(b.curOutput.Contents)-1].Key
		}
		b.curInput.Marker = aws.String(marker)
	} else {
		b.curInput = nil
	}

	return true

}

// ListObjectsOutput returns the next object in the result
func (b *BucketLister) ListObjectsOutput() *s3.ListObjectsOutput {
	return b.curOutput
}
