package dedup

import (
	"context"
	"fmt"
	"io"
	"os"
	"sync"

	minio "github.com/minio/minio/cmd"
	"github.com/zhengshuai-xiao/S3Store/internal"
	S3client "github.com/zhengshuai-xiao/S3Store/pkg/s3client"
)

const (
	bufferSize = 4 * 1024 * 1024
	maxSize    = 16 * 1024 * 1024
	filePath   = "/dedup/"
)

var buffPool = sync.Pool{
	New: func() interface{} {
		buf := make([]byte, bufferSize)
		return &buf
	},
}

type fpinDObj struct {
	fp     string
	offset int64
	len    int64
}

type DObj struct {
	bucket     string
	dobj_key   string
	path       string
	dob_offset uint64
	filer      *os.File
	fps        []fpinDObj
}

func (x *XlatorDedup) truncateDObj(ctx context.Context, dobj *DObj) (err error) {
	fps_off := dobj.dob_offset
	//write fps
	for _, fp := range dobj.fps {
		err = internal.SerializeToFile(fp, dobj.filer)
		if err != nil {
			return err
		}
	}
	//write offset
	b := internal.UInt64ToBytesLittleEndian(fps_off)
	_, err = dobj.filer.WriteAt(b[:], 0)
	if err != nil {
		return fmt.Errorf("failed to write fps_off[%u]: %w", fps_off, err)
	}
	dobj.filer.Close()
	//put obj to backend
	_, err = S3client.UploadFile(ctx, x.Client, dobj.bucket, dobj.dobj_key, dobj.path)
	//UploadFile(ctx context.Context, core *miniogo.Core, bucket, object, localFilePath string) (miniogo.UploadInfo, error)
	if err != nil {
		return fmt.Errorf("failed to upload file[%s]: %w", dobj.path, err)
	}

	return nil
}
func (x *XlatorDedup) newDObj(dobj *DObj) (err error) {
	doid, err := x.Mdsclient.GetIncreasedDOID()
	if err != nil {
		logger.Errorf("failed to GetIncreasedDOID, err:%s", err)
		return
	}
	dobj.dobj_key = x.Mdsclient.GetDObjNameInMDS(doid)
	dobj.path = filePath + dobj.dobj_key
	dobj.filer, err = os.OpenFile(dobj.path, os.O_WRONLY|os.O_CREATE|os.O_TRUNC, 0644)
	if err != nil {
		return err
	}
	var buf_off [8]byte
	dobj.filer.Write(buf_off[:])
	dobj.dob_offset = 8
	dobj.fps = []fpinDObj{}
	logger.Tracef("newDObj, dobj_key:%s", dobj.dobj_key)

	return nil
}

func (x *XlatorDedup) writeDObj(ctx context.Context, dobj *DObj, buf []byte, chunks []Chunk) (err error) {
	if dobj.dobj_key == "" {
		err = x.newDObj(dobj)
		if err != nil {
			return
		}
	}
	for i, _ := range chunks {
		if chunks[i].Deduped {
			continue
		}
		if dobj.dob_offset+uint64(chunks[i].Len) >= maxSize {
			err = x.truncateDObj(ctx, dobj)
			if err != nil {
				return
			}

			err = x.newDObj(dobj)
			if err != nil {
				return
			}
		}

		_, err = internal.WriteAll(dobj.filer, buf[chunks[i].off:chunks[i].off+chunks[i].Len])
		if err != nil {
			return
		}
		doid, err := x.Mdsclient.GetDOIDFromDObjName(dobj.dobj_key)
		if err != nil {
			return err
		}
		chunks[i].DOid = uint64(doid)
		chunks[i].OffInDOid = dobj.dob_offset
		chunks[i].LenInDOid = chunks[i].Len
		dobj.dob_offset += uint64(chunks[i].Len)
	}
	return err
}

func (x *XlatorDedup) writeObj(ctx context.Context, r *minio.PutObjReader, objInfo *minio.ObjectInfo) (err error) {
	totalsize := 0
	//TODO:
	cdc := FixedCDC{Chunksize: 128 * 1024}
	var buf = buffPool.Get().(*[]byte)
	defer buffPool.Put(buf)
	dobj := &DObj{bucket: BackendBucket, dobj_key: "", path: "", dob_offset: 0, filer: nil}
	for {
		var n int
		n, err = io.ReadFull(r, *buf)
		if n == 0 {
			if err == io.EOF {
				err = nil
			}
			break
		}
		//chunk
		chunks, err := cdc.Chunking((*buf)[:n])
		if err != nil {
			return err
		}
		//calc fp
		CalcFPs((*buf)[:n], chunks)
		//search fp
		err = x.Mdsclient.DedupFPs(chunks)
		if err != nil {
			return err
		}
		//write data
		err = x.writeDObj(ctx, dobj, (*buf)[:n], chunks)
		if err != nil {
			break
		}
		//append to manifest
		err = x.Mdsclient.WriteManifest(objInfo.UserTags, chunks)
		if err != nil {
			return err
		}

	}
	err = x.truncateDObj(ctx, dobj)
	if err != nil {
		return
	}
	objInfo.Size = int64(totalsize)

	return nil
}
