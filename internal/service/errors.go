package service

import "errors"

var (
	ErrInvalidArgument         = errors.New("invalid argument")
	ErrShipmentNotFound        = errors.New("shipment not found")
	ErrShipmentAlreadyExists   = errors.New("shipment already exists")
	ErrInvalidStatusTransition = errors.New("invalid shipment status transition")
)
