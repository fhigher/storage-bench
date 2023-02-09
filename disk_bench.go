package main

import (
	"bytes"
	"encoding/json"
	"flag"
	"fmt"
	"log"
	"math/rand"
	"os"
	"path/filepath"
	"strconv"
	"sync"
	"syscall"
	"time"
)

// TempFile file path
type TempFile string

// GetFileSize get file size
func (f TempFile) GetFileSize() (fileSize int64, err error) {
	finfo, err := os.Stat(f.String())
	if nil != err {
		return
	}

	//log.Println(finfo)
	return finfo.Size(), nil
}

func (f TempFile) String() string {
	return string(f)
}

// FileSizeLimit file max length
//const FileSizeLimit int64 = 32 << 30

// WriteFile write rand number to file
func (f TempFile) WriteFile(fileSizeLimit int64, i int) (length int64, err error) {
	rand.Seed(time.Now().UnixNano())
	var (
		file *os.File
		buf  = new(bytes.Buffer)
		n    int
	)
	file, err = os.OpenFile(f.String(), os.O_CREATE|os.O_RDWR, os.ModePerm)
	if nil != err {
		return
	}

	defer file.Close()

	for {
		if length >= fileSizeLimit {
			break
		}

		str := strconv.Itoa(rand.Int())

		// 使用buf，比直接写入，速度提升了N倍
		buf.WriteString(str)
		n, err = file.WriteString(buf.String())
		if nil != err {
			return
		}

		//buf.Reset() 会变的极其慢，和使用bufio.NewWrite()速度一样慢，原因有待研究
		length += int64(n)

		fmt.Printf("\rWriteProcess: %.0f%c", float64(length)/float64(fileSizeLimit)*100, '%')
	}
	return
}

var filePath = flag.String("path", "", "--path=file path ")
var fileUnit = flag.Int64("size", 1, "--size=file size, the unit is GB, eg: --size=1, it is 1GB")
var concurrent = flag.Int("con", 1, "--con=file number, the number of files generated concurrently")
var jsonFile = flag.Bool("json", false, "--json, output json file")
var deleteFile = flag.Bool("clean", false, "--clean, auto delete bench file")
var help = flag.Bool("h", false, "show help")

var helpUsage = func() {
	fmt.Println(`
help info:
	--path  specify file store path, not filename
	--size  specify file size. the unit is GB, eg: --size=1, it is 1GB
	--con   specify the number of files generated concurrently
	--json  output json file
	--clean auto delete bench file
	--h    show this info
	`)
}

// FileFlag file flag param
type FileFlag struct {
	FilePath   string
	FileUnit   int64
	Concurrent int
}

// FileCountInfo file
type FileCountInfo struct {
	FileName  string
	FileSize  int64
	UsedTime  time.Duration
	ByteCount int64
	WriteRate float64
}

// Report report
type Report struct {
	FileInfo         []*FileCountInfo
	AverageWriteRate string
	AverageUsedTime  string
}

func main() {
	flag.Usage = helpUsage
	flag.Parse()

	if *help {
		flag.Usage()
		os.Exit(0)
	}

	if *filePath == "" {
		flag.Usage()
		os.Exit(0)
	}

	fileFlag := &FileFlag{
		FilePath:   *filePath,
		FileUnit:   *fileUnit,
		Concurrent: *concurrent,
	}
	fileFlag.FilePath = filepath.Join(fileFlag.FilePath, "bench_file")
	// 创建生成文件的目录
	err := os.MkdirAll(fileFlag.FilePath, os.ModePerm)
	if nil != err {
		log.Panicln(err)
	}

	// 检测当前空间是否满足存储
	enough, err := CheckSpaceEnough(fileFlag)
	if nil != err {
		log.Printf("CheckSpaceEnough: %s", err)
	}
	if !enough {
		os.Exit(0)
	}
	// 并发写入
	log.Println("Start writing ......")
	fmt.Println()
	concurrentWrite := &ConcurrentWrite{}
	for i := 0; i < fileFlag.Concurrent; i++ {
		concurrentWrite.Wg.Add(1)
		go concurrentWrite.Write(i, fileFlag)
	}

	concurrentWrite.Wg.Wait()
	fmt.Println()
	fmt.Println()
	time.Sleep(100 * time.Millisecond)

	var (
		totalWriteRate float64
		totalUsedTime  float64
	)
	for _, info := range concurrentWrite.FileInfo {
		fmt.Printf("%s: \n", info.FileName)
		fmt.Printf("\tFileName:  %s\n", info.FileName)
		fmt.Printf("\tFileSize:  %dGB(%d)\n", info.FileSize>>30, info.FileSize)
		fmt.Printf("\tByteCount: %dGB(%d)\n", info.ByteCount>>30, info.ByteCount)
		fmt.Printf("\tUsedTime:  %s\n", info.UsedTime.String())
		fmt.Printf("\tWriteRate: %.2fM/s\n", info.WriteRate)

		totalWriteRate += info.WriteRate
		totalUsedTime += info.UsedTime.Seconds()
	}

	fmt.Printf("Count: \n")
	averageWriteRate := fmt.Sprintf("%.2fM/s", totalWriteRate/float64(fileFlag.Concurrent))
	fmt.Printf("\tAverageWriteRate: %s\n", averageWriteRate)
	averageUsedTime := fmt.Sprintf("%.2fs", totalUsedTime/float64(fileFlag.Concurrent))
	fmt.Printf("\tAverageUsedTime: %s", averageUsedTime)

	fmt.Println()
	log.Println("All file finished")

	if *jsonFile {
		report := Report{
			FileInfo:         concurrentWrite.FileInfo,
			AverageWriteRate: averageWriteRate,
			AverageUsedTime:  averageUsedTime,
		}

		f, err := os.OpenFile("bench-report.json", os.O_RDWR|os.O_CREATE, os.ModePerm)
		if nil != err {
			log.Panicln(err)
		}
		if err = json.NewEncoder(f).Encode(&report); nil != err {
			log.Panicln(err)
		}
	}

	if *deleteFile {
		if err = os.RemoveAll(fileFlag.FilePath); nil != err {
			log.Println(err)
		}
	}

}

// ConcurrentWrite concurrent
type ConcurrentWrite struct {
	Wg       sync.WaitGroup
	FileInfo []*FileCountInfo
	Lc       sync.Mutex
}

// Write write
func (c *ConcurrentWrite) Write(i int, flagParam *FileFlag) {
	defer c.Wg.Done()

	filename := filepath.Join(flagParam.FilePath, fmt.Sprintf("random_file_%d", i))
	file := TempFile(filename)
	filesize := flagParam.FileUnit << 30
	start := time.Now()

	bytecount, err := file.WriteFile(filesize, i)
	if nil != err {
		log.Printf("write file(%s): %s\n", filename, err)
		return
	}

	end := time.Now()
	useTime := end.Sub(start)
	writeRate := float64(bytecount>>20) / useTime.Seconds()
	info := &FileCountInfo{
		FileName:  filename,
		FileSize:  filesize,
		UsedTime:  useTime,
		ByteCount: bytecount,
		WriteRate: writeRate,
	}

	c.Lc.Lock()
	c.FileInfo = append(c.FileInfo, info)
	c.Lc.Unlock()
}

// GetPathSpace get path space
func GetPathSpace(path string) (stat *syscall.Statfs_t, err error) {
	stat = &syscall.Statfs_t{}
	if err = syscall.Statfs(path, stat); nil != err {
		return
	}

	return
}

// PrintPathStatInfo info
func PrintPathStatInfo(stat *syscall.Statfs_t, needResource uint64) (total, avail uint64) {
	fmt.Printf("Path Space: \n")
	total = stat.Blocks * uint64(stat.Bsize)
	avail = stat.Bavail * uint64(stat.Bsize)
	fmt.Printf("\tTotal: %dGB(%d)\n", total>>30, total)
	fmt.Printf("\tAvail: %dGB(%d)\n", avail>>30, avail)
	fmt.Printf("\tNeed:  %dGB(%d)\n", needResource>>30, needResource)
	fmt.Println()

	return
}

// CheckSpaceEnough enough
func CheckSpaceEnough(fileFlag *FileFlag) (enough bool, err error) {
	stat, err := GetPathSpace(fileFlag.FilePath)
	if nil != err {
		return
	}

	needResource := uint64(fileFlag.FileUnit << 30 * int64(fileFlag.Concurrent))

	_, avail := PrintPathStatInfo(stat, needResource)

	if needResource > avail {
		log.Printf("not enough space, avail: %dGB(%d), need: %dGB(%d), diff: %dGB(%d)",
			avail>>30, avail, needResource>>30, needResource, (needResource-avail)>>30, needResource-avail)
		return
	}

	return true, nil
}
