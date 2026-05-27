package controller

import (
	"context"

	"k8s.io/client-go/kubernetes"
)

func Run(ctx context.Context, clientset kubernetes.Interface) error {
	<-ctx.Done()
	return nil
}
