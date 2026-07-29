package vless

import (
	"encoding/binary"
	"fmt"
	"math/rand/v2"
	"runtime"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"testing"

	"github.com/puzpuzpuz/xsync/v4"
	"github.com/xtls/xray-core/common/errors"
	"github.com/xtls/xray-core/common/protocol"
	"github.com/xtls/xray-core/common/uuid"
)

const n int64 = 100000

func setupUsers(size int64) []*protocol.MemoryUser {
	users := make([]*protocol.MemoryUser, size)
	for i := int64(0); i < size; i++ {
		users[i] = &protocol.MemoryUser{Email: fmt.Sprintf("%d@example.com", i)}
	}
	return users
}

func setupXMap(size int64) (*xsync.Map[string, *protocol.MemoryUser], []*protocol.MemoryUser) {
	xMap := xsync.NewMap[string, *protocol.MemoryUser]()
	users := setupUsers(size)
	for _, u := range users {
		xMap.Store(u.Email, u)
	}
	return xMap, users
}

func setupMap(size int64) (*sync.Map, []*protocol.MemoryUser) {
	m := &sync.Map{}
	users := setupUsers(size)
	for _, u := range users {
		m.Store(u.Email, u)
	}
	return m, users
}

func BenchmarkXMap(b *testing.B) {
	xMap, users := setupXMap(n)
	b.ResetTimer()
	b.RunParallel(func(pb *testing.PB) {
		i := int64(0)
		for pb.Next() {
			want := users[i%n]
			val, _ := xMap.Load(want.Email)
			if val != want {
				b.Errorf("Load(%q) returned the wrong user", want.Email)
			}
			i++
		}
	})
}

func BenchmarkMap(b *testing.B) {
	m, users := setupMap(n)
	b.ResetTimer()
	b.RunParallel(func(pb *testing.PB) {
		i := int64(0)
		for pb.Next() {
			want := users[i%n]
			val, _ := m.Load(want.Email)
			if val.(*protocol.MemoryUser) != want {
				b.Errorf("Load(%q) returned the wrong user", want.Email)
			}
			i++
		}
	})
}

var writeRatios = []int{1, 10, 50}

const churnKeys = 64

func newRNG(counter *atomic.Uint64) (*rand.Rand, uint64) {
	id := counter.Add(1)
	return rand.New(rand.NewPCG(42, id)), id
}

func BenchmarkXMapMixed(b *testing.B) {
	for _, pct := range writeRatios {
		b.Run(fmt.Sprintf("writes=%d%%", pct), func(b *testing.B) {
			xMap, users := setupXMap(n)
			var seed atomic.Uint64
			b.ResetTimer()
			b.RunParallel(func(pb *testing.PB) {
				rng, _ := newRNG(&seed)
				for pb.Next() {
					want := users[rng.Uint64N(uint64(n))]
					if int(rng.Uint64N(100)) < pct {
						xMap.Store(want.Email, want)
						continue
					}
					if val, ok := xMap.Load(want.Email); ok && val != want {
						b.Errorf("Load(%q) returned the wrong user", want.Email)
					}
				}
			})
		})
	}
}

func BenchmarkMapMixed(b *testing.B) {
	for _, pct := range writeRatios {
		b.Run(fmt.Sprintf("writes=%d%%", pct), func(b *testing.B) {
			m, users := setupMap(n)
			var seed atomic.Uint64
			b.ResetTimer()
			b.RunParallel(func(pb *testing.PB) {
				rng, _ := newRNG(&seed)
				for pb.Next() {
					want := users[rng.Uint64N(uint64(n))]
					if int(rng.Uint64N(100)) < pct {
						m.Store(want.Email, want)
						continue
					}
					if val, ok := m.Load(want.Email); ok && val.(*protocol.MemoryUser) != want {
						b.Errorf("Load(%q) returned the wrong user", want.Email)
					}
				}
			})
		})
	}
}

// BenchmarkXMapChurn and BenchmarkMapChurn model the validator's real write
// pattern: readers look up a stable user set while other goroutines add and
// remove users. Inserting keys that are absent from the map forces sync.Map to
// take its mutex and periodically rebuild the read-only copy from the dirty map.
func BenchmarkXMapChurn(b *testing.B) {
	for _, pct := range writeRatios {
		b.Run(fmt.Sprintf("writes=%d%%", pct), func(b *testing.B) {
			xMap, users := setupXMap(n)
			var seed atomic.Uint64
			b.ResetTimer()
			b.RunParallel(func(pb *testing.PB) {
				rng, id := newRNG(&seed)
				prefix := "churn" + strconv.FormatUint(id, 10) + "-"
				i := int64(0)
				for pb.Next() {
					if int(rng.Uint64N(100)) < pct {
						key := prefix + strconv.FormatInt(i%churnKeys, 10)
						if i%(2*churnKeys) < churnKeys {
							xMap.Store(key, users[i%n])
						} else {
							xMap.Delete(key)
						}
						i++
						continue
					}
					want := users[rng.Uint64N(uint64(n))]
					if val, ok := xMap.Load(want.Email); ok && val != want {
						b.Errorf("Load(%q) returned the wrong user", want.Email)
					}
				}
			})
		})
	}
}

func BenchmarkMapChurn(b *testing.B) {
	for _, pct := range writeRatios {
		b.Run(fmt.Sprintf("writes=%d%%", pct), func(b *testing.B) {
			m, users := setupMap(n)
			var seed atomic.Uint64
			b.ResetTimer()
			b.RunParallel(func(pb *testing.PB) {
				rng, id := newRNG(&seed)
				prefix := "churn" + strconv.FormatUint(id, 10) + "-"
				i := int64(0)
				for pb.Next() {
					if int(rng.Uint64N(100)) < pct {
						key := prefix + strconv.FormatInt(i%churnKeys, 10)
						if i%(2*churnKeys) < churnKeys {
							m.Store(key, users[i%n])
						} else {
							m.Delete(key)
						}
						i++
						continue
					}
					want := users[rng.Uint64N(uint64(n))]
					if val, ok := m.Load(want.Email); ok && val.(*protocol.MemoryUser) != want {
						b.Errorf("Load(%q) returned the wrong user", want.Email)
					}
				}
			})
		})
	}
}

var benchSizes = []int64{1000, 100000, 1000000}

type validatorImpl struct {
	name string
	new  func() Validator
}

var validatorImpls = []validatorImpl{
	{"rwmutex", func() Validator { return newRWMutexValidator() }},
	{"sync.Map", func() Validator { return newSyncMapValidator() }},
	{"xsync.Map", func() Validator { return NewMemoryValidator() }},
}

type rwMutexValidator struct {
	sync.RWMutex
	email map[string]*protocol.MemoryUser
	users map[[16]byte]*protocol.MemoryUser
}

func newRWMutexValidator() *rwMutexValidator {
	return &rwMutexValidator{
		email: make(map[string]*protocol.MemoryUser),
		users: make(map[[16]byte]*protocol.MemoryUser),
	}
}

func (v *rwMutexValidator) Add(u *protocol.MemoryUser) error {
	v.Lock()
	defer v.Unlock()
	if u.Email != "" {
		le := strings.ToLower(u.Email)
		if _, found := v.email[le]; found {
			return errors.New("User ", u.Email, " already exists.")
		}
		v.email[le] = u
	}
	v.users[ProcessUUID(u.Account.(*MemoryAccount).ID.UUID())] = u
	return nil
}

func (v *rwMutexValidator) Del(e string) error {
	if e == "" {
		return errors.New("Email must not be empty.")
	}
	le := strings.ToLower(e)
	v.Lock()
	defer v.Unlock()
	u := v.email[le]
	if u == nil {
		return errors.New("User ", e, " not found.")
	}
	delete(v.email, le)
	delete(v.users, ProcessUUID(u.Account.(*MemoryAccount).ID.UUID()))
	return nil
}

func (v *rwMutexValidator) Get(id uuid.UUID) *protocol.MemoryUser {
	v.RLock()
	defer v.RUnlock()
	return v.users[ProcessUUID(id)]
}

func (v *rwMutexValidator) GetByEmail(email string) *protocol.MemoryUser {
	email = strings.ToLower(email)
	v.RLock()
	defer v.RUnlock()
	return v.email[email]
}

func (v *rwMutexValidator) GetAll() []*protocol.MemoryUser {
	v.RLock()
	defer v.RUnlock()
	u := make([]*protocol.MemoryUser, 0, len(v.email))
	for _, user := range v.email {
		u = append(u, user)
	}
	return u
}

func (v *rwMutexValidator) GetCount() int64 {
	v.RLock()
	defer v.RUnlock()
	return int64(len(v.email))
}

type syncMapValidator struct {
	email sync.Map
	users sync.Map
	count int64
}

func newSyncMapValidator() *syncMapValidator {
	return &syncMapValidator{}
}

func (v *syncMapValidator) Add(u *protocol.MemoryUser) error {
	if u.Email != "" {
		_, loaded := v.email.LoadOrStore(strings.ToLower(u.Email), u)
		if loaded {
			return errors.New("User ", u.Email, " already exists.")
		}
		atomic.AddInt64(&v.count, 1)
	}
	v.users.Store(ProcessUUID(u.Account.(*MemoryAccount).ID.UUID()), u)
	return nil
}

func (v *syncMapValidator) Del(e string) error {
	if e == "" {
		return errors.New("Email must not be empty.")
	}
	le := strings.ToLower(e)
	u, _ := v.email.Load(le)
	if u == nil {
		return errors.New("User ", e, " not found.")
	}
	v.email.Delete(le)
	atomic.AddInt64(&v.count, -1)
	v.users.Delete(ProcessUUID(u.(*protocol.MemoryUser).Account.(*MemoryAccount).ID.UUID()))
	return nil
}

func (v *syncMapValidator) Get(id uuid.UUID) *protocol.MemoryUser {
	u, _ := v.users.Load(ProcessUUID(id))
	if u != nil {
		return u.(*protocol.MemoryUser)
	}
	return nil
}

func (v *syncMapValidator) GetByEmail(email string) *protocol.MemoryUser {
	u, _ := v.email.Load(strings.ToLower(email))
	if u != nil {
		return u.(*protocol.MemoryUser)
	}
	return nil
}

func (v *syncMapValidator) GetAll() []*protocol.MemoryUser {
	u := make([]*protocol.MemoryUser, 0, 100)
	v.email.Range(func(_, value any) bool {
		u = append(u, value.(*protocol.MemoryUser))
		return true
	})
	return u
}

func (v *syncMapValidator) GetCount() int64 {
	return atomic.LoadInt64(&v.count)
}

func benchUsers(size int64) []*protocol.MemoryUser {
	users := make([]*protocol.MemoryUser, size)
	for i := int64(0); i < size; i++ {
		var id uuid.UUID
		binary.BigEndian.PutUint64(id[8:], uint64(i)+1)
		users[i] = &protocol.MemoryUser{
			Email:   fmt.Sprintf("user%d@example.com", i),
			Account: &MemoryAccount{ID: protocol.NewID(id)},
		}
	}
	return users
}

func setupValidator(impl validatorImpl, size int64) (Validator, []*protocol.MemoryUser) {
	v := impl.new()
	users := benchUsers(size)
	for _, u := range users {
		if err := v.Add(u); err != nil {
			panic(err)
		}
	}
	return v, users
}

func sizeLabel(size int64) string {
	switch {
	case size%1000000 == 0:
		return strconv.FormatInt(size/1000000, 10) + "M"
	case size%1000 == 0:
		return strconv.FormatInt(size/1000, 10) + "k"
	default:
		return strconv.FormatInt(size, 10)
	}
}

func runValidatorBench(b *testing.B, prepare func(users []*protocol.MemoryUser) func(v Validator, i int64) *protocol.MemoryUser) {
	for _, impl := range validatorImpls {
		for _, size := range benchSizes {
			b.Run(fmt.Sprintf("%s/users=%s", impl.name, sizeLabel(size)), func(b *testing.B) {
				v, users := setupValidator(impl, size)
				lookup := prepare(users)
				b.ResetTimer()
				b.RunParallel(func(pb *testing.PB) {
					i := int64(0)
					for pb.Next() {
						idx := i % size
						if got := lookup(v, idx); got != users[idx] {
							b.Errorf("lookup of user %d returned the wrong user", idx)
						}
						i++
					}
				})
			})
		}
	}
}

func BenchmarkValidatorGetByUUID(b *testing.B) {
	runValidatorBench(b, func(users []*protocol.MemoryUser) func(Validator, int64) *protocol.MemoryUser {
		ids := make([]uuid.UUID, len(users))
		for i, u := range users {
			ids[i] = u.Account.(*MemoryAccount).ID.UUID()
		}
		return func(v Validator, i int64) *protocol.MemoryUser {
			return v.Get(ids[i])
		}
	})
}

func BenchmarkValidatorGetByEmail(b *testing.B) {
	runValidatorBench(b, func(users []*protocol.MemoryUser) func(Validator, int64) *protocol.MemoryUser {
		emails := make([]string, len(users))
		for i, u := range users {
			emails[i] = u.Email
		}
		return func(v Validator, i int64) *protocol.MemoryUser {
			return v.GetByEmail(emails[i])
		}
	})
}

func BenchmarkValidatorAddByEmailAndUUID(b *testing.B) {
	for _, impl := range validatorImpls {
		for _, size := range benchSizes {
			b.Run(fmt.Sprintf("%s/users=%s", impl.name, sizeLabel(size)), func(b *testing.B) {
				users := benchUsers(size)
				b.ReportAllocs()
				b.ResetTimer()
				for b.Loop() {
					v := impl.new()
					for _, u := range users {
						if err := v.Add(u); err != nil {
							b.Fatal(err)
						}
					}
				}
				b.ReportMetric(float64(b.Elapsed().Nanoseconds())/float64(b.N*int(size)), "ns/user")
			})
		}
	}
}

func churnUsers(gid uint64) []*protocol.MemoryUser {
	users := make([]*protocol.MemoryUser, churnKeys)
	for k := range users {
		var id uuid.UUID
		binary.BigEndian.PutUint32(id[0:], uint32(gid))
		binary.BigEndian.PutUint16(id[4:], uint16(k))
		users[k] = &protocol.MemoryUser{
			Email:   fmt.Sprintf("churn%d-%d@example.com", gid, k),
			Account: &MemoryAccount{ID: protocol.NewID(id)},
		}
	}
	return users
}

func BenchmarkValidatorMixedByUUID(b *testing.B) {
	for _, impl := range validatorImpls {
		for _, size := range benchSizes {
			for _, pct := range writeRatios {
				b.Run(fmt.Sprintf("%s/users=%s/writes=%d%%", impl.name, sizeLabel(size), pct), func(b *testing.B) {
					v, users := setupValidator(impl, size)
					ids := make([]uuid.UUID, size)
					for i, u := range users {
						ids[i] = u.Account.(*MemoryAccount).ID.UUID()
					}
					pool := make([][]*protocol.MemoryUser, runtime.GOMAXPROCS(0))
					for i := range pool {
						pool[i] = churnUsers(uint64(i) + 1)
					}
					var seed atomic.Uint64
					b.ResetTimer()
					b.RunParallel(func(pb *testing.PB) {
						rng, gid := newRNG(&seed)
						churn := pool[(gid-1)%uint64(len(pool))]
						j := int64(0)
						for pb.Next() {
							if int(rng.Uint64N(100)) < pct {
								u := churn[j%churnKeys]
								if (j/churnKeys)%2 == 0 {
									_ = v.Add(u)
								} else {
									_ = v.Del(u.Email)
								}
								j++
								continue
							}
							idx := rng.Uint64N(uint64(size))
							if got := v.Get(ids[idx]); got != users[idx] {
								b.Errorf("Get returned the wrong user for index %d", idx)
							}
						}
					})
				})
			}
		}
	}
}
