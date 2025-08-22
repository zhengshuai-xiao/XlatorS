package dedup

import (
	"context"
	"encoding/gob"
	"fmt"
	"io"
	"os"
	"sync"
	"time"

	miniogo "github.com/minio/minio-go/v7"
	minio "github.com/minio/minio/cmd"
	"github.com/zhengshuai-xiao/XlatorS/internal"
	S3client "github.com/zhengshuai-xiao/XlatorS/pkg/s3client"
)

const (
	bufferSize = 16 * 1024 * 1024
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
	FP     [32]byte
	Offset uint64
	Len    uint64
}

type DObj struct {
	bucket     string
	dobj_key   string
	path       string
	dob_offset uint64
	filer      *os.File
	fps        []fpinDObj
}

type DObjReader struct {
	bucket     string
	dobj_key   string
	path       string
	dob_offset uint64
	filer      *os.File
	fpmap      map[string]fpinDObj
}

func (x *XlatorDedup) truncateDObj(ctx context.Context, dobj *DObj) (err error) {
	fps_off := dobj.dob_offset
	if fps_off <= 8 {
		logger.Tracef("truncateDObj:nothing need to write")
		return nil
	}
	//write fps
	seekCurrent, _ := dobj.filer.Seek(0, io.SeekCurrent)
	logger.Tracef("xzs SeekCurrent=%d", seekCurrent)
	encoder := gob.NewEncoder(dobj.filer)
	for _, fp := range dobj.fps {
		err = encoder.Encode(fp)
		if err != nil {
			return err
		}
		seekCurrent, _ = dobj.filer.Seek(0, io.SeekCurrent)
		logger.Tracef("xzs SeekCurrent=%d", seekCurrent)
	}

	logger.Tracef("truncateDObj:write %d fps to %s", len(dobj.fps), dobj.dobj_key)
	//write offset
	b := internal.UInt64ToBytesLittleEndian(fps_off)
	_, err = dobj.filer.WriteAt(b[:], 0)
	if err != nil {
		return fmt.Errorf("failed to write fps_off[%d]: %w", fps_off, err)
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
func (x *XlatorDedup) getDobjPathFromLocal(dobj_key string) string {
	//read data from object
	return filePath + dobj_key
}
func (x *XlatorDedup) newDObj(dobj *DObj) (err error) {
	doid, err := x.Mdsclient.GetIncreasedDOID()
	if err != nil {
		logger.Errorf("failed to GetIncreasedDOID, err:%s", err)
		return
	}
	dobj.dobj_key = x.Mdsclient.GetDObjNameInMDS(uint64(doid))
	dobj.path = x.getDobjPathFromLocal(dobj.dobj_key)
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

func (x *XlatorDedup) writeDObj(ctx context.Context, dobj *DObj, buf []byte, chunks []Chunk) (int, error) {
	var writenLen int = 0
	var err error
	if dobj.dobj_key == "" {
		err = x.newDObj(dobj)
		if err != nil {
			return 0, err
		}
	}

	for i, _ := range chunks {
		if chunks[i].Deduped {
			continue
		}
		if dobj.dob_offset+uint64(chunks[i].Len) >= maxSize {
			err = x.truncateDObj(ctx, dobj)
			if err != nil {
				return 0, err
			}

			err = x.newDObj(dobj)
			if err != nil {
				return 0, err
			}
		}

		len, err := internal.WriteAll(dobj.filer, buf[chunks[i].off:chunks[i].off+chunks[i].Len])
		if err != nil {
			return 0, err
		}
		writenLen += len
		doid, err := x.Mdsclient.GetDOIDFromDObjName(dobj.dobj_key)
		if err != nil {
			return 0, err
		}
		chunks[i].DOid = uint64(doid)
		//chunks[i].OffInDOid = dobj.dob_offset
		//chunks[i].LenInDOid = chunks[i].Len
		fp := fpinDObj{
			Offset: dobj.dob_offset,
			Len:    chunks[i].Len,
		}
		copy(fp.FP[:], chunks[i].FP[:])
		dobj.fps = append(dobj.fps, fp)

		dobj.dob_offset += uint64(chunks[i].Len)

	}
	return writenLen, err
}

func (x *XlatorDedup) writeObj(ctx context.Context, r *minio.PutObjReader, objInfo *minio.ObjectInfo) (manifestList []ChunkInManifest, err error) {
	var totalSize int64 = 0
	var totalWriteSize int64 = 0
	start := time.Now()
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
		totalSize += int64(n)
		//chunk
		chunks, err := cdc.Chunking((*buf)[:n])
		if err != nil {
			logger.Errorf("writeObj: failed to chunk data: %s", err)
			return nil, err
		}
		//calc fp
		CalcFPs((*buf)[:n], chunks)
		//search fp
		err = x.Mdsclient.DedupFPsBatch(chunks)
		if err != nil {
			logger.Errorf("writeObj: failed to deduplicate chunks: %s", err)
			return nil, err
		}
		//write data
		n, err = x.writeDObj(ctx, dobj, (*buf)[:n], chunks)
		if err != nil {
			logger.Errorf("writeObj: failed to writeDObj: %s", err)
			break
		}
		totalWriteSize += int64(n)
		//append to manifest
		for _, chunk := range chunks {
			manifestList = append(manifestList, ChunkInManifest{
				FP:   chunk.FP,
				Len:  chunk.Len,
				DOid: chunk.DOid,
			})
		}
	}
	err = x.truncateDObj(ctx, dobj)
	if err != nil {
		logger.Errorf("writeObj: failed to truncateDObj: %s", err)
		return
	}
	objInfo.Size = int64(totalSize)
	dedupRate := float64(totalSize-totalWriteSize) / float64(totalSize)
	objInfo.UserDefined["wroteSize"] = fmt.Sprintf("%d", totalWriteSize)
	objInfo.UserDefined["dedupRate"] = fmt.Sprintf("%.2f%%", dedupRate*100)
	logger.Infof("writeDObj: wroteSize/totalSize %d/%d bytes, dedupRate: %s", totalWriteSize, totalSize, objInfo.UserDefined["dedupRate"])

	elapsed := time.Since(start)
	logger.Infof("writeDObj: elapsed time %s", elapsed)

	return manifestList, nil
}

func (x *XlatorDedup) readDataFromObject(writer io.Writer, chunk ChunkInManifest, dobjReader *DObjReader) (err error) {
	//read data from object
	fpinDObj := dobjReader.fpmap[string(chunk.FP[:])]
	logger.Tracef("readDataFromObject: fpinDObj: %+v", fpinDObj)
	_, err = dobjReader.filer.Seek(int64(fpinDObj.Offset), 0)
	if err != nil {
		logger.Errorf("readDataFromObject: failed to seek file err: %s", err)
		return
	}
	_, err = io.CopyN(writer, dobjReader.filer, int64(fpinDObj.Len))
	if err != nil {
		logger.Errorf("readDataFromObject: failed to copy file err: %s", err)
		return
	}
	return
}
func (x *XlatorDedup) parseDataObject(dobjReader *DObjReader) (err error) {
	// parse the file
	var fpOffsetStr [8]byte
	_, err = dobjReader.filer.Read(fpOffsetStr[:])
	if err != nil {
		logger.Errorf("parseDataObject: failed to read fpOffsetStr err: %s", err)
		return
	}

	fpOffset := internal.BytesToUInt64LittleEndian(fpOffsetStr)
	logger.Tracef("parseDataObject: read fpOffset: %d", fpOffset)

	//var buf []byte
	//var buf bytes.Buffer
	//buffer := make([]byte, 4096)
	dobjReader.filer.Seek(int64(fpOffset), 0)
	/*for {
		n, err := dobjReader.filer.Read(buffer)
		if n > 0 {
			buf.Write(buffer[:n])
		}
		if err == io.EOF {
			break
		}
		if err != nil {
			return err
		}
		logger.Tracef("xzs 1, %d", buf.Len())
	}*/

	//buf_ := bytes.NewBufferString(string(buf[:]))
	//gob.Register(fpinDObj{})

	decoder := gob.NewDecoder(dobjReader.filer)
	for {
		var fpinDObj fpinDObj

		if err = decoder.Decode(&fpinDObj); err != nil {
			if err == io.EOF {
				break
			}
			logger.Errorf("parseDataObject: failed to DeserializeFromString [%s] err: %s", dobjReader.path, err)
			return err
		}

		dobjReader.fpmap[string(fpinDObj.FP[:])] = fpinDObj
		logger.Tracef("xzs 2:fpinDObj=(%v)", fpinDObj)
	}

	return nil
}

func (x *XlatorDedup) getDataObject(bucket, object string, o minio.ObjectOptions) (dobjReader DObjReader, err error) {
	ctx := context.Background()
	//check if the file exist
	//init the fpmap
	dobjReader.fpmap = make(map[string]fpinDObj)
	dobjReader.path = x.getDobjPathFromLocal(object)
	_, err = os.Stat(dobjReader.path)
	if err != nil {
		if os.IsNotExist(err) {
			//download from backend
			opts := miniogo.GetObjectOptions{}
			opts.ServerSideEncryption = o.ServerSideEncryption
			dobjCloseReader, _, _, err := x.Client.GetObject(ctx, bucket, object, opts)
			if err != nil {
				logger.Errorf("failed to get object[%s] from backend: %s", object, err)
				return DObjReader{}, err
			}
			_, err = internal.WriteReadCloserToFile(dobjCloseReader, dobjReader.path)
			if err != nil {
				logger.Errorf("failed to write object[%s] to local disk: %s", object, err)
				return DObjReader{}, err
			}
		}
		return
	}
	logger.Tracef("read data object[%s] on local disk", dobjReader.path)
	dobjReader.bucket = bucket
	dobjReader.dobj_key = object
	//open data object
	dobjReader.filer, err = os.Open(dobjReader.path)
	if err != nil {
		logger.Errorf("getDataObject: failed to open data object[%s] err: %s", dobjReader.path, err)
		return
	}

	// parse the file
	err = x.parseDataObject(&dobjReader)
	if err != nil {
		logger.Errorf("getDataObject: failed to parse data object[%s] err: %s", dobjReader.path, err)
		return
	}

	return dobjReader, nil
}
func (x *XlatorDedup) readDataObject(chunks []ChunkInManifest, startOffset, length int64, writer io.Writer, o minio.ObjectOptions) (err error) {
	//parse chunks
	logger.Tracef("readDataObject enter:%d", len(chunks))
	for _, chunk := range chunks {
		//download and read data object
		var dobjReader DObjReader
		dobjReader, err = x.getDataObject(BackendBucket, x.Mdsclient.GetDObjNameInMDS(chunk.DOid), o)
		if err != nil {
			logger.Errorf("readDataObject: failed to get data object[%d] err: %s", chunk.DOid, err)
			return
		}
		//read data from object
		err = x.readDataFromObject(writer, chunk, &dobjReader)
		if err != nil {
			logger.Errorf("readDataObject: failed to read data from object[%d] err: %s", chunk.DOid, err)
			return
		}
	}
	return
}

func (x *XlatorDedup) readObject(ctx context.Context, bucket, object string, startOffset, length int64, writer io.Writer, etag string, opts minio.ObjectOptions) (err error) {
	logger.Tracef("readObject enter")
	manifest, err := x.Mdsclient.GetObjectManifest(bucket, object)
	if err != nil {
		logger.Errorf("readObject: failed to get object manifest[%s] err: %s", object, err)
		return
	}
	logger.Tracef("readObject manifest=%d", len(manifest))
	err = x.readDataObject(manifest, startOffset, length, writer, opts)
	if err != nil {
		logger.Errorf("readObject: failed to read data object[%s] err: %s", object, err)
		return
	}
	logger.Tracef("readObject end")
	return
}
