// Code generated by go-swagger; DO NOT EDIT.

package operations

// This file was generated by the swagger tool.
// Editing this file might prove futile when you re-run the swagger generate command

import (
	"net/http"

	"github.com/go-openapi/errors"
	"github.com/go-openapi/runtime"
	"github.com/go-openapi/runtime/middleware"

	strfmt "github.com/go-openapi/strfmt"

	"github.com/treeverse/lakefs/api/gen/models"
)

// NewCommitParams creates a new CommitParams object
// no default values defined in spec.
func NewCommitParams() CommitParams {

	return CommitParams{}
}

// CommitParams contains all the bound params for the commit operation
// typically these are obtained from a http.Request
//
// swagger:parameters commit
type CommitParams struct {

	// HTTP Request Object
	HTTPRequest *http.Request `json:"-"`

	/*
	  Required: true
	  In: path
	*/
	BranchID string
	/*
	  In: body
	*/
	Commit *models.CommitCreation
	/*
	  Required: true
	  In: path
	*/
	RepositoryID string
}

// BindRequest both binds and validates a request, it assumes that complex things implement a Validatable(strfmt.Registry) error interface
// for simple values it will use straight method calls.
//
// To ensure default values, the struct must have been initialized with NewCommitParams() beforehand.
func (o *CommitParams) BindRequest(r *http.Request, route *middleware.MatchedRoute) error {
	var res []error

	o.HTTPRequest = r

	rBranchID, rhkBranchID, _ := route.Params.GetOK("branchId")
	if err := o.bindBranchID(rBranchID, rhkBranchID, route.Formats); err != nil {
		res = append(res, err)
	}

	if runtime.HasBody(r) {
		defer r.Body.Close()
		var body models.CommitCreation
		if err := route.Consumer.Consume(r.Body, &body); err != nil {
			res = append(res, errors.NewParseError("commit", "body", "", err))
		} else {
			// validate body object
			if err := body.Validate(route.Formats); err != nil {
				res = append(res, err)
			}

			if len(res) == 0 {
				o.Commit = &body
			}
		}
	}
	rRepositoryID, rhkRepositoryID, _ := route.Params.GetOK("repositoryId")
	if err := o.bindRepositoryID(rRepositoryID, rhkRepositoryID, route.Formats); err != nil {
		res = append(res, err)
	}

	if len(res) > 0 {
		return errors.CompositeValidationError(res...)
	}
	return nil
}

// bindBranchID binds and validates parameter BranchID from path.
func (o *CommitParams) bindBranchID(rawData []string, hasKey bool, formats strfmt.Registry) error {
	var raw string
	if len(rawData) > 0 {
		raw = rawData[len(rawData)-1]
	}

	// Required: true
	// Parameter is provided by construction from the route

	o.BranchID = raw

	return nil
}

// bindRepositoryID binds and validates parameter RepositoryID from path.
func (o *CommitParams) bindRepositoryID(rawData []string, hasKey bool, formats strfmt.Registry) error {
	var raw string
	if len(rawData) > 0 {
		raw = rawData[len(rawData)-1]
	}

	// Required: true
	// Parameter is provided by construction from the route

	o.RepositoryID = raw

	return nil
}