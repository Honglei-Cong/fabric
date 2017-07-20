package rbft

type Persist struct {
	db *DB
}

func NewPersist(dir string) (*Persist, error) {
	db := createDB(dir)
	if err := db.Open(); err != nil {
		return nil, err
	}
	return &Persist{
		db: db,
	}, nil
}

func (p *Persist) StoreState(key string, value []byte) error {
	return p.db.Put([]byte(key), value, true)
}

func (p *Persist) ReadState(key string) ([]byte, error) {
	return p.db.Get([]byte(key))
}

func (p *Persist) DelState(key string) {
	p.db.Delelte([]byte(key))
}

func (p *Persist) ReadStateSet(prefix string) (map[string][]byte, error) {
	r := make(map[string][]byte)
	return r, nil
}
