package main

import (
	"database/sql"
	"errors"
	"log"
)

type ParcelStore struct {
	db *sql.DB
}

func NewParcelStore(db *sql.DB) ParcelStore {
	return ParcelStore{db: db}
}

func (s ParcelStore) Add(p Parcel) (int, error) {
	res, err := s.db.Exec(
		"INSERT INTO parcel (client, status, address, created_at) VALUES (:client, :status, :address, :created_at)",
		sql.Named("client", p.Client),
		sql.Named("address", p.Address),
		sql.Named("status", p.Status),
		sql.Named("created_at", p.CreatedAt),
	)
	if err != nil {
		log.Printf("Add: failed to insert parcel %v", err)
		return 0, err
	}

	id, err := res.LastInsertId()
	if err != nil {
		log.Printf("Add: failed to last insert if: %v", err)
		return 0, err
	}

	return int(id), nil
}

func (s ParcelStore) Get(number int) (Parcel, error) {

	row := s.db.QueryRow(
		"SELECT number, client, status, address, created_at FROM parcel WHERE number = :number",
		sql.Named("number", number),
	)

	p := Parcel{}
	err := row.Scan(&p.Number, &p.Client, &p.Status, &p.Address, &p.CreatedAt)
	if err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			log.Printf("Get: parcel %d not found", number)
		} else {
			log.Printf("Get:failed to scan parcel %d: %v", number, err)
		}
		return Parcel{}, err
	}
	return p, nil
}

func (s ParcelStore) GetByClient(client int) ([]Parcel, error) {
	rows, err := s.db.Query(
		"SELECT number, client, status, address, created_at FROM parcel WHERE client = :client",
		sql.Named("client", client),
	)
	if err != nil {
		log.Printf("GetByClient: query failed for client %d: %v", client, err)
		return nil, err
	}
	defer rows.Close()

	var res []Parcel

	for rows.Next() {
		var p Parcel
		if err := rows.Scan(&p.Number, &p.Client, &p.Status, &p.Address, &p.CreatedAt); err != nil {
			return nil, err
		}
		res = append(res, p)
	}

	if err = rows.Err(); err != nil {
		log.Printf("GetByClient: rows iteration error for client %d: %v", client, err)
		return nil, err
	}

	return res, nil
}

func (s ParcelStore) SetStatus(number int, status string) error {
	_, err := s.db.Exec(
		"UPDATE parcel SET status = :status WHERE number = :number",
		sql.Named("status", status),
		sql.Named("number", number),
	)
	if err != nil {
		log.Printf("SetStatus: failed to update status for parcel %d: %v", number, err)
	}
	return err
}

func (s ParcelStore) SetAddress(number int, address string) error {
	res, err := s.db.Exec(
		"UPDATE parcel SET address = :address WHERE number = :number AND status = :status",
		sql.Named("address", address),
		sql.Named("number", number),
		sql.Named("status", ParcelStatusRegistered),
	)
	if err != nil {
		log.Printf("SetAddress: failed to update parcel %d: %v", number, err)
	}
	return err

	rowsAffected, err := res.RowsAffected()
	if err != nil {
		log.Printf("SetAddress: failed to get rows affected for parcel %d: %v", number, err)
		return err
	}
	if rowsAffected == 0 {
		log.Printf("SetAddress: no rows affected for parcel %d", number)
		return errors.New("the address can only be changed for a registered parcel")
	}
	return nil
}

func (s ParcelStore) Delete(number int) error {
	res, err := s.db.Exec(
		"DELETE FROM parcel WHERE number = :number AND status = :status",
		sql.Named("number", number),
		sql.Named("status", ParcelStatusRegistered),
	)
	if err != nil {
		log.Printf("Delete: failed to delete parcel %d: %v", number, err)
		return err
	}
	rowsAffected, err := res.RowsAffected()
	if err != nil {
		log.Printf("Delete: failed to get rows affected for parcel %d: %v", number, err)
		return err
	}

	if rowsAffected == 0 {
		log.Printf("Delete: no rows affected for parcel %d", number)
		return errors.New("only a registered address can be deleted")
	}

	return nil
}
