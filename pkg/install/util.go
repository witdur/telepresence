package install

import (
	"errors"
	"fmt"

	"github.com/datawire/ambassador/pkg/kates"

	"github.com/hashicorp/go-multierror"
	corev1 "k8s.io/api/core/v1"
)

func GetPodTemplateFromObject(obj kates.Object) (*kates.PodTemplateSpec, error) {
	var tplSpec *kates.PodTemplateSpec
	switch obj := obj.(type) {
	case *kates.ReplicaSet:
		tplSpec = &obj.Spec.Template
	case *kates.Deployment:
		tplSpec = &obj.Spec.Template
	case *kates.StatefulSet:
		tplSpec = &obj.Spec.Template
	default:
		return nil, ObjErrorf(obj, "unsupported workload kind %q", obj.GetObjectKind().GroupVersionKind().Kind)
	}
	return tplSpec, nil
}

// GetPort finds a port with the given name and returns it.
func GetPort(cn *corev1.Container, portName string) (*corev1.ContainerPort, error) {
	ports := cn.Ports
	for pn := range ports {
		p := &ports[pn]
		if p.Name == portName {
			return p, nil
		}
	}
	return nil, fmt.Errorf("unable to locate port %q in container %q", portName, cn.Name)
}

func ObjErrorf(obj kates.Object, format string, args ...interface{}) error {
	return fmt.Errorf("%s name=%q namespace=%q: %w",
		obj.GetObjectKind().GroupVersionKind().Kind, obj.GetName(), obj.GetNamespace(),
		fmt.Errorf(format, args...))
}

// AlreadyUndone means that an install action has already been undone, perhaps by manual user action
type AlreadyUndone struct {
	err error
	msg string
}

func (e *AlreadyUndone) Error() string {
	return fmt.Sprintf("%s: %v", e.msg, e.err)
}

func (e *AlreadyUndone) Unwrap() error {
	return e.err
}

func NewAlreadyUndone(err error, msg string) error {
	return &AlreadyUndone{err, msg}
}

// IsAlreadyUndone returns whether the given error -- possibly a multierror -- indicates that all actions have been undone.
func IsAlreadyUndone(err error) bool {
	var undone *AlreadyUndone
	if errors.As(err, &undone) {
		return true
	}
	var multi *multierror.Error
	if !errors.As(err, &multi) {
		return false
	}
	for _, err := range multi.WrappedErrors() {
		if !errors.As(err, &undone) {
			return false
		}
	}
	return true
}
