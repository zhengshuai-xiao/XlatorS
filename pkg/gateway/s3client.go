package gateway

import (
	"context"
	"math/rand"
	"net/http"
	"net/url"
	"time"

	miniogo "github.com/minio/minio-go/v7"
	"github.com/minio/minio-go/v7/pkg/credentials"
	"github.com/minio/minio-go/v7/pkg/s3utils"
	minio "github.com/minio/minio/cmd"
	"github.com/minio/minio/pkg/auth"
)

/*
type S3Credential struct {
	endpoint        string
	accessKeyID     string
	secretAccessKey string
	useSSL          bool
}

type S3Client struct {
	Client  *miniogo.Client
	s3creds S3Credential
}

func NewS3Client(s3creds S3Credential) (s3client *S3Client, err error) {

	// Initialize minio client object.
	s3client.s3creds.endpoint = s3creds.endpoint
	s3client.s3creds.accessKeyID = s3creds.accessKeyID
	s3client.s3creds.secretAccessKey = s3creds.secretAccessKey
	s3client.s3creds.useSSL = s3creds.useSSL

	s3client.Client, err = miniogo.New(s3creds.endpoint, &miniogo.Options{
		Creds:  credentials.NewStaticV4(s3creds.accessKeyID, s3creds.secretAccessKey, ""),
		Secure: s3creds.useSSL,
	})
	if err != nil {
		//logger.Errorf("failed to create S3 client: %v", err)
		return nil, err
	}
	return s3client, nil
}
*/
// S3 implements Gateway.
type S3 struct {
	host string
}

// Name implements Gateway interface.
func (g *S3) Name() string {
	return minio.S3BackendGateway
}

const letterBytes = "abcdefghijklmnopqrstuvwxyz01234569"
const (
	letterIdxBits = 6                    // 6 bits to represent a letter index
	letterIdxMask = 1<<letterIdxBits - 1 // All 1-bits, as many as letterIdxBits
	letterIdxMax  = 63 / letterIdxBits   // # of letter indices fitting in 63 bits
)

// randString generates random names and prepends them with a known prefix.
func randString(n int, src rand.Source, prefix string) string {
	b := make([]byte, n)
	// A rand.Int63() generates 63 random bits, enough for letterIdxMax letters!
	for i, cache, remain := n-1, src.Int63(), letterIdxMax; i >= 0; {
		if remain == 0 {
			cache, remain = src.Int63(), letterIdxMax
		}
		if idx := int(cache & letterIdxMask); idx < len(letterBytes) {
			b[i] = letterBytes[idx]
			i--
		}
		cache >>= letterIdxBits
		remain--
	}
	return prefix + string(b[0:30-len(prefix)])
}

// Chains all credential types, in the following order:
//   - AWS env vars (i.e. AWS_ACCESS_KEY_ID)
//   - AWS creds file (i.e. AWS_SHARED_CREDENTIALS_FILE or ~/.aws/credentials)
//   - Static credentials provided by user (i.e. MINIO_ROOT_USER)
var defaultProviders = []credentials.Provider{
	&credentials.EnvAWS{},
	&credentials.FileAWSCredentials{},
	&credentials.EnvMinio{},
}

// Chains all credential types, in the following order:
//   - AWS env vars (i.e. AWS_ACCESS_KEY_ID)
//   - AWS creds file (i.e. AWS_SHARED_CREDENTIALS_FILE or ~/.aws/credentials)
//   - IAM profile based credentials. (performs an HTTP
//     call to a pre-defined endpoint, only valid inside
//     configured ec2 instances)
var defaultAWSCredProviders = []credentials.Provider{
	&credentials.EnvAWS{},
	&credentials.FileAWSCredentials{},
	&credentials.IAM{
		Client: &http.Client{
			Transport: minio.NewGatewayHTTPTransport(),
		},
	},
	&credentials.EnvMinio{},
}

// newS3 - Initializes a new client by auto probing S3 server signature.
func newS3(urlStr string, tripper http.RoundTripper) (*miniogo.Core, error) {
	if urlStr == "" {
		urlStr = "https://s3.amazonaws.com"
	}

	u, err := url.Parse(urlStr)
	if err != nil {
		return nil, err
	}

	// Override default params if the host is provided
	endpoint, secure, err := minio.ParseGatewayEndpoint(urlStr)
	if err != nil {
		return nil, err
	}

	var creds *credentials.Credentials
	if s3utils.IsAmazonEndpoint(*u) {
		// If we see an Amazon S3 endpoint, then we use more ways to fetch backend credentials.
		// Specifically IAM style rotating credentials are only supported with AWS S3 endpoint.
		creds = credentials.NewChainCredentials(defaultAWSCredProviders)

	} else {
		creds = credentials.NewChainCredentials(defaultProviders)
	}

	options := &miniogo.Options{
		Creds:        creds,
		Secure:       secure,
		Region:       s3utils.GetRegionFromURL(*u),
		BucketLookup: miniogo.BucketLookupAuto,
		Transport:    tripper,
	}

	clnt, err := miniogo.New(endpoint, options)
	if err != nil {
		return nil, err
	}

	return &miniogo.Core{Client: clnt}, nil
}

// NewGatewayLayer returns s3 ObjectLayer.
func (g *S3) NewGatewayLayer(creds auth.Credentials, s3Objects *S3SObjects) error {
	metrics := minio.NewMetrics()

	t := &minio.MetricsTransport{
		Transport: minio.NewGatewayHTTPTransport(),
		Metrics:   metrics,
	}

	// creds are ignored here, since S3 gateway implements chaining
	// all credentials.
	clnt, err := newS3(g.host, t)
	if err != nil {
		return err
	}

	probeBucketName := randString(60, rand.NewSource(time.Now().UnixNano()), "probe-bucket-sign-")

	// Check if the provided keys are valid.
	if _, err = clnt.BucketExists(context.Background(), probeBucketName); err != nil {
		if miniogo.ToErrorResponse(err).Code != "AccessDenied" {
			return err
		}
	}
	s3Objects.Client = clnt
	s3Objects.Metrics = metrics
	s3Objects.HTTPClient = &http.Client{
		Transport: t,
	}

	// Enables single encryption of KMS is configured.
	if minio.GlobalKMS != nil {
		/*
			encS := s3EncObjects{s}

			// Start stale enc multipart uploads cleanup routine.
			go encS.cleanupStaleEncMultipartUploads(minio.GlobalContext,
				minio.GlobalStaleUploadsCleanupInterval, minio.GlobalStaleUploadsExpiry)
		*/
		logger.Errorf("Failed to enable single encryption of KMS is configured")
		return nil
	}
	return nil
}

// Production - s3 gateway is production ready.
func (g *S3) Production() bool {
	return true
}
