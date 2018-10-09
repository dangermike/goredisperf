package main

import (
	"errors"
	"fmt"
	"math/rand"
	"os"
	"sort"
	"sync"
	"time"

	"github.com/go-redis/redis"
	"github.com/urfave/cli"
)

func main() {
	app := cli.NewApp()
	app.Name = "goredisperf"
	app.Usage = "Multithreaded Redis MGET tester"
	cli.HelpFlag = cli.BoolFlag{
		Name:  "help, ?",
		Usage: "show help",
	}

	app.Flags = []cli.Flag{
		cli.StringFlag{
			Name:  "host, h",
			Value: "127.0.0.1",
			Usage: "Server hostname",
		},
		cli.IntFlag{
			Name:  "port, p",
			Value: 6379,
			Usage: "Server port",
		},
		cli.StringFlag{
			Name:  "password, a",
			Value: "",
			Usage: "Password to use when connecting to the server",
		},
		cli.IntFlag{
			Name:  "db, n",
			Value: 0,
			Usage: "Database number",
		},
		cli.IntFlag{
			Name:  "cycles",
			Value: 100,
			Usage: "Number of attempts for each key count",
		},
		cli.IntFlag{
			Name:  "data-size",
			Value: 2048,
			Usage: "Size of test data values, in bytes",
		},
	}

	app.Commands = []cli.Command{
		cli.Command{
			Name:   "concurrency",
			Usage:  "Test various key counts at various levels of concurrency",
			Action: concurrencyAction,
			Flags: []cli.Flag{
				cli.IntFlag{
					Name:  "min-conc",
					Value: 1,
					Usage: "Minimum concurrency",
				},
				cli.IntFlag{
					Name:  "max-conc",
					Value: 16,
					Usage: "Maximum concurrency",
				},
			},
		},
		cli.Command{
			Name:   "scatter",
			Usage:  "Output key-count vs. time points, optionally plotting",
			Action: scatterAction,
			Flags: []cli.Flag{
				cli.IntFlag{
					Name:  "concurrency",
					Value: 1,
					Usage: "Concurrency",
				},
				cli.IntFlag{
					Name:  "min-keys",
					Value: 1,
					Usage: "Minimum number of keys to fetch in a cycle",
				},
				cli.IntFlag{
					Name:  "max-keys",
					Value: 100,
					Usage: "Maximum number of keys to fetch in a cycle",
				},
				cli.BoolFlag{
					Name:  "gnuplot",
					Usage: "Output GnuPlot script for scatter",
				},
				cli.StringSliceFlag{
					Name:  "gnuplot-extra",
					Usage: "Inject additional commands into the gnuplot render",
				},
			},
		},
	}
	if err := app.Run(os.Args); err != nil {
		panic(err)
	}
}

func concurrencyAction(ctx *cli.Context) error {
	if ctx.Int("min-conc") < 1 {
		return errors.New("min-conc must be greater than zero")
	}
	if ctx.Int("min-conc") > ctx.Int("max-conc") {
		return errors.New("min-conc cannot exceed max-conc")
	}
	if ctx.GlobalInt("cycles") < 1 {
		return errors.New("cycles must be greater than 0")
	}

	client := redis.NewClient(ctxToRedisOptions(ctx))
	defer client.Close() //nolint

	if _, err := clearTestKeys(client); err != nil {
		panic(err)
	}

	keys, err := getTestKeys(client, ctx.GlobalInt("data-size"))
	if err != nil {
		panic(err)
	}
	fmt.Printf("Holding %d keys\n", len(keys))

	counts := []int{1, 1}
	for i := 5; i <= 100; i += 5 {
		counts = append(counts, i)
	}
	concs := []int{}
	for c := ctx.Int("min-conc"); c <= ctx.Int("max-conc"); c <<= 1 {
		concs = append(concs, c)
	}
	fmt.Print("keys")
	for _, c := range concs {
		fmt.Printf("\tc=%d", c)
	}
	fmt.Println()
	for _, cnt := range counts {
		res := make([]int64, ctx.GlobalInt("cycles"))
		fmt.Print(cnt)
		for _, conc := range concs {
			var wg sync.WaitGroup
			indices := make(chan int) // indices into the result slice
			for t := 0; t < conc; t++ {
				wg.Add(1)
				go func() {
					client.Get("fake") // warm up client
					defer wg.Done()
					mykeys := make([]string, len(keys)) // copy for safety
					copy(mykeys, keys)
					for ix := range indices { // ix is the index where the duration will be written
						shuffleKeys(mykeys)
						start := time.Now()
						cmd := client.MGet(mykeys[:cnt]...)
						dur := time.Since(start)
						if _, rerr := cmd.Result(); rerr != nil {
							panic(rerr)
						}
						res[ix] = dur.Nanoseconds()
					}
				}()
			}
			for c := 0; c < ctx.GlobalInt("cycles"); c++ {
				indices <- c
			}
			close(indices)
			wg.Wait()
			sortInt64(res)
			fmt.Printf("\t%0.3f", medianInt64(res)/1000000.0)
		}
		fmt.Println()
	}

	cnt, err := clearTestKeys(client)
	fmt.Printf("Deleted %d test keys\n", cnt)

	return err
}

type scatterItem struct {
	keys     int
	duration time.Duration
}

func scatterAction(ctx *cli.Context) error {
	if ctx.Int("min-keys") < 1 {
		return errors.New("min-keys must be greater than zero")
	}
	if ctx.Int("min-keys") > ctx.Int("max-keys") {
		return errors.New("min-keys cannot exceed max-keys")
	}
	if ctx.GlobalInt("cycles") < 1 {
		return errors.New("cycles must be greater than 0")
	}
	if ctx.Int("concurrency") < 1 {
		return errors.New("concurrency must be greater than 0")
	}

	client := redis.NewClient(ctxToRedisOptions(ctx))
	defer client.Close() //nolint

	if _, err := clearTestKeys(client); err != nil {
		panic(err)
	}

	keys, err := getTestKeys(client, ctx.GlobalInt("data-size"))
	if err != nil {
		panic(err)
	}

	if ctx.Bool("gnuplot") {
		fmt.Println("$DATABLOCK << EOD")
	}

	var wgWorkers sync.WaitGroup
	var wgWriter sync.WaitGroup
	indices := make(chan int) // indices into the result slice
	outchan := make(chan scatterItem)
	minKeys := ctx.Int("min-keys")
	maxKeys := ctx.Int("max-keys")
	keyRange := maxKeys - minKeys

	wgWriter.Add(1)
	go func() {
		defer wgWriter.Done()
		for si := range outchan {
			fmt.Printf("%d\t%0.3f\n", si.keys, float64(si.duration.Nanoseconds())/1000000.0)
		}
	}()
	for t := 0; t < ctx.Int("concurrency"); t++ {
		wgWorkers.Add(1)
		go func() {
			client.Get("fake") // warm up client
			defer wgWorkers.Done()
			mykeys := make([]string, len(keys)) // copy for safety
			copy(mykeys, keys)
			for range indices {
				keyCnt := minKeys
				if keyRange > 0 {
					keyCnt += rand.Intn(keyRange)
				}
				shuffleKeys(mykeys)
				start := time.Now()
				cmd := client.MGet(mykeys[:keyCnt]...)
				dur := time.Since(start)
				if _, rerr := cmd.Result(); rerr != nil {
					panic(rerr)
				}
				outchan <- scatterItem{keyCnt, dur}
			}
		}()
	}
	for c := 0; c < ctx.GlobalInt("cycles"); c++ {
		indices <- c
	}
	close(indices)
	wgWorkers.Wait()
	close(outchan)
	wgWriter.Wait()

	if ctx.Bool("gnuplot") {
		fmt.Println("EOD")
		fmt.Println(`set fit nolog`)
		fmt.Println(`set fit quiet`)
		fmt.Println(`set term pngcairo size 1280, 1024 font "sans,16"`)
		fmt.Println(`set xlabel "key count"`)
		fmt.Println(`set ylabel "time (ms)"`)
		for _, line := range ctx.StringSlice("gnuplot-extra") {
			fmt.Println(line)
		}
		fmt.Println(`f(x) = a*x+b`)
		fmt.Println(`fit f(x) $DATABLOCK via a,b`)
		fmt.Printf("plot $DATABLOCK title \"mget (c=%d)\", f(x) with lines lw 3 title sprintf(\"y = %%0.6fx + %%0.6f\", a, b)\n", ctx.Int("concurrency"))
	}
	_, err = clearTestKeys(client)
	return err
}

func ctxToRedisOptions(ctx *cli.Context) *redis.Options {
	return &redis.Options{
		Addr:     fmt.Sprintf("%s:%d", ctx.GlobalString("host"), ctx.GlobalInt("port")),
		Password: ctx.GlobalString("password"),
		DB:       ctx.GlobalInt("db"),
	}
}

func getTestKeys(client *redis.Client, valueSize int) ([]string, error) {
	var keys []string

	iter := client.Scan(0, "test_*", 5000).Iterator()
	for iter.Next() {
		keys = append(keys, iter.Val())
	}
	buf := make([]byte, valueSize)
	for len(keys) < 50000 {
		key := fmt.Sprintf("test_%05d", len(keys))
		if _, err := rand.Read(buf); err != nil {
			return nil, err
		}
		if _, err := client.Set(key, buf, time.Duration(0)).Result(); err != nil {
			return nil, err
		}
		keys = append(keys, key)
	}
	return keys, nil
}

func shuffleKeys(keys []string) {
	rand.Shuffle(len(keys), func(i, j int) {
		keys[i], keys[j] = keys[j], keys[i]
	})
}

func clearTestKeys(client *redis.Client) (int64, error) {
	total := int64(0)

	kcnt := -1
	for kcnt != 0 {
		var keys []string
		iter := client.Scan(0, "test_*", 5000).Iterator()
		for iter.Next() {
			keys = append(keys, iter.Val())
		}
		kcnt = len(keys)
		if kcnt > 0 {
			cnt, err := client.Del(keys...).Result()
			total += cnt
			if err != nil {
				return total, err
			}
		}
	}
	return total, nil
}

func sortInt64(arr []int64) {
	sort.Slice(arr, func(i, j int) bool {
		return arr[i] < arr[j]
	})
}

func medianInt64(data []int64) float64 {
	l := len(data)
	if l%2 == 0 {
		return float64(data[l/2-1]+data[l/2+1]) / 2.0
	}
	return float64(data[l/2])
}
