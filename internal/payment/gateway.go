package payment

import (
	"context"
	"errors"
)

type PaymentGateway interface {
	Charge(ctx context.Context, orderID string, amount float32) (approved bool, err error)
}

type SimulatedPaymentGateway struct{}

func NewSimulatedPaymentGateway() *SimulatedPaymentGateway {
	return &SimulatedPaymentGateway{}
}

func (g *SimulatedPaymentGateway) Charge(ctx context.Context, orderID string, amount float32) (approved bool, err error) {
	if amount > 200.0 {
		return false, nil
	} else if amount > 0 {
		return true, nil
	}
	return false, errors.New("invalid payment")
}
