package dedup

import (
	"fmt"
	"strings"
)

// ParseNamespaceAndBucket splits a full name string like "namespace.bucket"
// into its constituent parts. If the separator '.' is not found, the namespace
// will be empty and the bucketName will be the full input string.
// It returns the namespace, the bucket name, and an error if the format is invalid.
func ParseNamespaceAndBucket(fullName string) (namespace, bucketName string, err error) {
	parts := strings.SplitN(fullName, ".", 2)
	if len(parts) == 1 {
		// No separator found, treat the whole string as the bucket name.
		return DefaultNS, DefaultNS + "." + fullName, nil
	}

	// Separator found, butp one part is empty (e.g., ".bucket" or "ns.")
	if parts[0] == "" || parts[1] == "" {
		return "", "", fmt.Errorf("invalid format: namespace or bucket name cannot be empty in '%s'", fullName)
	}

	namespace = parts[0]
	bucketName = fullName
	return
}

// GetBackendBucketName constructs the name for the backend bucket based on a namespace.
// It uses a fixed prefix "Dedup.".
func GetBackendBucketName(namespace string) string {
	return BackendBucketPrefix + namespace
}

func GetBackendBucketNameViaBucketName(bucket string) string {
	ns, _, err := ParseNamespaceAndBucket(bucket)
	if err != nil {
		logger.Errorf("Get backend bucket name failed: %s", err)
		return ""
	}
	return BackendBucketPrefix + ns
}

func GetFingerprintCache(namespace string) string {
	return namespace + "." + FPCacheKey
}

func GetRefKey(namespace string) string {
	return namespace + "." + RefKeySuffix
}
