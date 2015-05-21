package awsutils

import (
	"fmt"
	"testing"

	"github.com/awslabs/aws-sdk-go/aws"
	"github.com/awslabs/aws-sdk-go/service/s3"
	"github.com/stretchr/testify/assert"
)

func newObject(key string) *s3.Object {
	return &s3.Object{
		Key: aws.String(key),
	}
}

type TestObjectLister struct {
}

func (o *TestObjectLister) ListObjects(input *s3.ListObjectsInput) (*s3.ListObjectsOutput, error) {

	return &s3.ListObjectsOutput{
		Contents: []*s3.Object{
			newObject("obj0"),
			newObject("obj1"),
		},
		IsTruncated: aws.Boolean(false),
	}, nil

}

func TestBucketLister(t *testing.T) {
	bl := &BucketLister{
		s3:       &TestObjectLister{},
		curInput: &s3.ListObjectsInput{},
	}

	cnt := 0
	for bl.Next() {
		for i, o := range bl.ListObjectsOutput().Contents {
			cnt++
			assert.Equal(t, fmt.Sprintf("obj%d", i), *o.Key)
		}
	}

	assert.Nil(t, bl.Err())
	assert.Equal(t, 2, cnt)

}
