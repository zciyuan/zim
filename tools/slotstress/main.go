// Command slotstress verifies concurrent slot allocation and the 255 cap.
package main

import (
	"context"
	"flag"
	"fmt"
	"os"
	"sync"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"

	devicedao "github.com/zciyuan/ZIM/internal/dao/device"
)

func main() {
	dsn := flag.String("dsn", "postgres://zim:zim@127.0.0.1:5432/zim?sslmode=disable", "postgres dsn")
	uid := flag.Int64("uid", 990001, "synthetic owner id")
	devices := flag.Int("devices", 300, "concurrent device count")
	flag.Parse()

	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	pool, err := pgxpool.New(ctx, *dsn)
	if err != nil {
		fail("pool: %v", err)
	}
	defer pool.Close()
	if _, err = pool.Exec(ctx, `DELETE FROM zim_device WHERE owner_id = $1`, *uid); err != nil {
		fail("clean: %v", err)
	}
	dao := devicedao.New(pool)

	slots := make([]int16, 0, *devices)
	var mu sync.Mutex
	var wg sync.WaitGroup
	var failures int
	for i := 0; i < *devices; i++ {
		wg.Add(1)
		go func(index int) {
			defer wg.Done()
			slot, allocErr := dao.EnsureSlot(ctx, *uid, 2, fmt.Sprintf("stress-%03d", index))
			mu.Lock()
			defer mu.Unlock()
			if allocErr != nil {
				failures++
				return
			}
			slots = append(slots, slot)
		}(i)
	}
	wg.Wait()

	seen := make(map[int16]bool, len(slots))
	for _, slot := range slots {
		if seen[slot] {
			fail("duplicate slot %d", slot)
		}
		seen[slot] = true
	}
	fmt.Printf("devices=%d allocated=%d failures=%d unique=%d\n", *devices, len(slots), failures, len(seen))
	if len(slots) != 255 || failures != *devices-255 {
		fail("allocation cap mismatch")
	}

	// The same device id must always reuse its slot under concurrency.
	const sameDevice = "stress-000"
	sameSlots := make(chan int16, 50)
	for i := 0; i < 50; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			slot, allocErr := dao.EnsureSlot(ctx, *uid, 2, sameDevice)
			if allocErr != nil {
				fail("same device: %v", allocErr)
			}
			sameSlots <- slot
		}()
	}
	wg.Wait()
	close(sameSlots)
	var first int16
	for slot := range sameSlots {
		if first == 0 {
			first = slot
		} else if slot != first {
			fail("same device got different slots: %d/%d", first, slot)
		}
	}
	if _, err = pool.Exec(ctx, `DELETE FROM zim_device WHERE owner_id = $1`, *uid); err != nil {
		fail("cleanup: %v", err)
	}
	fmt.Printf("PASS cap=255 unique=255 same_device_slot=%d\n", first)
}

func fail(format string, args ...any) {
	fmt.Fprintf(os.Stderr, format+"\n", args...)
	os.Exit(1)
}
