// Copyright (c) 2025 马晓璐
//
// Permission is hereby granted, free of charge, to any person obtaining a copy of
// this software and associated documentation files (the "Software"), to deal in
// the Software without restriction, including without limitation the rights to
// use, copy, modify, merge, publish, distribute, sublicense, and/or sell copies of
// the Software, and to permit persons to whom the Software is furnished to do so,
// subject to the following conditions:
//
// The above copyright notice and this permission notice shall be included in all
// copies or substantial portions of the Software.
//
// THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
// IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY, FITNESS
// FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE AUTHORS OR
// COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER LIABILITY, WHETHER
// IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM, OUT OF OR IN
// CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE SOFTWARE.

package v1

import (
	"time"

	"github.com/maxiaolu1981/cretem/nexuscore/component-base/scheme"
)

type ObjectMetaAccessor interface {
	GetObjectMeta() Object
}

// Object lets you work with object metadata from any of the versioned or
// internal API objects. Attempting to set or retrieve a field on an object that does
// not support that field (Name, UID, Namespace on lists) will be a no-op and return
// a default value.
type Object interface {
	GetID() uint64
	SetID(id uint64)
	GetName() string
	SetName(name string)
	GetCreatedAt() time.Time
	SetCreatedAt(createdAt time.Time)
	GetUpdatedAt() time.Time
	SetUpdatedAt(updatedAt time.Time)
}

// ListInterface lets you work with list metadata from any of the versioned or
// internal API objects. Attempting to set or retrieve a field on an object that does
// not support that field will be a no-op and return a default value.
type ListInterface interface {
	GetTotalCount() int64
	SetTotalCount(count int64)
}

// Type exposes the type and APIVersion of versioned or internal API objects.
type Type interface {
	GetAPIVersion() string
	SetAPIVersion(version string)
	GetKind() string
	SetKind(kind string)
}

var _ ListInterface = &ListMeta{}

func (meta *ListMeta) GetTotalCount() int64      { return meta.TotalCount }
func (meta *ListMeta) SetTotalCount(count int64) { meta.TotalCount = count }

var _ Type = &TypeMeta{}

func (obj *TypeMeta) GetObjectKind() scheme.ObjectKind { return obj }

// SetGroupVersionKind satisfies the ObjectKind interface for all objects that embed TypeMeta.
func (obj *TypeMeta) SetGroupVersionKind(gvk scheme.GroupVersionKind) {
	obj.APIVersion, obj.Kind = gvk.ToAPIVersionAndKind()
}

// GroupVersionKind satisfies the ObjectKind interface for all objects that embed TypeMeta.
func (obj *TypeMeta) GroupVersionKind() scheme.GroupVersionKind {
	return scheme.FromAPIVersionAndKind(obj.APIVersion, obj.Kind)
}

func (meta *TypeMeta) GetAPIVersion() string        { return meta.APIVersion }
func (meta *TypeMeta) SetAPIVersion(version string) { meta.APIVersion = version }
func (meta *TypeMeta) GetKind() string              { return meta.Kind }
func (meta *TypeMeta) SetKind(kind string)          { meta.Kind = kind }

func (obj *ListMeta) GetListMeta() ListInterface { return obj }

func (obj *ObjectMeta) GetObjectMeta() Object { return obj }

var _ Object = &ObjectMeta{}

func (meta *ObjectMeta) GetID() uint64                    { return meta.ID }
func (meta *ObjectMeta) SetID(id uint64)                  { meta.ID = id }
func (meta *ObjectMeta) GetName() string                  { return meta.Name }
func (meta *ObjectMeta) SetName(name string)              { meta.Name = name }
func (meta *ObjectMeta) GetCreatedAt() time.Time          { return meta.CreatedAt }
func (meta *ObjectMeta) SetCreatedAt(createdAt time.Time) { meta.CreatedAt = createdAt }
func (meta *ObjectMeta) GetUpdatedAt() time.Time          { return meta.UpdatedAt }
func (meta *ObjectMeta) SetUpdatedAt(updatedAt time.Time) { meta.UpdatedAt = updatedAt }
