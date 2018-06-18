package plugin

import (
	"encoding/json"
	"errors"

	"github.com/hashicorp/terraform/config/configschema"
	"github.com/hashicorp/terraform/plugin/proto"
	"github.com/hashicorp/terraform/tfdiags"
)

// schemaBlock takes the GetSchcema_Block from a grpc response and converts it
// to a terraform *configschema.Block.
func schemaBlock(b *proto.GetSchema_Block) *configschema.Block {
	block := &configschema.Block{
		Attributes: make(map[string]*configschema.Attribute),
		BlockTypes: make(map[string]*configschema.NestedBlock),
	}

	for _, a := range b.Attributes {
		attr := &configschema.Attribute{
			Description: a.Description,
			Required:    a.Required,
			Optional:    a.Optional,
			Computed:    a.Computed,
			Sensitive:   a.Sensitive,
		}

		if err := json.Unmarshal(a.Type, attr.Type); err != nil {
			panic(err)
		}

		block.Attributes[a.Name] = attr
	}

	for _, b := range b.BlockTypes {
		block.BlockTypes[b.TypeName] = schemaNestedBlock(b)
	}

	return block
}

func schemaNestedBlock(b *proto.GetSchema_NestedBlock) *configschema.NestedBlock {
	nb := &configschema.NestedBlock{
		Nesting:  configschema.NestingMode(b.Nesting),
		MinItems: int(b.MinItems),
		MaxItems: int(b.MaxItems),
	}

	nested := schemaBlock(b.Block)
	nb.Block = *nested
	return nb
}

// protoSchemaBlock takes a *configschema.Block and converts it to a
// GetSchema_Block for a grpc response.
func protoSchemaBlock(b *configschema.Block) *proto.GetSchema_Block {
	block := &proto.GetSchema_Block{}

	for name, a := range b.Attributes {
		attr := &proto.GetSchema_Attribute{
			Name:        name,
			Description: a.Description,
			Optional:    a.Optional,
			Computed:    a.Computed,
			Sensitive:   a.Sensitive,
		}

		ty, err := json.Marshal(a.Type)
		if err != nil {
			panic(err)
		}

		attr.Type = ty

		block.Attributes = append(block.Attributes, attr)
	}

	for name, b := range b.BlockTypes {
		block.BlockTypes = append(block.BlockTypes, protoSchemaNestedBlock(name, b))
	}

	return block
}

func protoSchemaNestedBlock(name string, b *configschema.NestedBlock) *proto.GetSchema_NestedBlock {
	return &proto.GetSchema_NestedBlock{
		TypeName: name,
		Block:    protoSchemaBlock(&b.Block),
		Nesting:  proto.GetSchema_NestingMode(b.Nesting),
		MinItems: int64(b.MinItems),
		MaxItems: int64(b.MaxItems),
	}
}

// diagnostics converts a list of proto.Diagnostics to a tf.Diagnostics.
// for now we assume these only contain a basic message
func diagnostics(ds []*proto.Diagnostic) tfdiags.Diagnostics {
	var diags tfdiags.Diagnostics
	for _, d := range ds {
		switch d.Level {
		case proto.Diagnostic_ERROR:
			diags = diags.Append(errors.New(d.Summary))
		case proto.Diagnostic_WARNING:
			diags = diags.Append(tfdiags.SimpleWarning(d.Summary))
		}
	}

	return diags
}

// warnsAndErrs converts a list of proto.Diagnostics to the legacy list of
// warning strings and errors.
func warnsAndErrs(ds []*proto.Diagnostic) ([]string, []error) {
	var warns []string
	var errs []error
	for _, d := range ds {
		switch d.Level {
		case proto.Diagnostic_ERROR:
			errs = append(errs, errors.New(d.Summary))
		case proto.Diagnostic_WARNING:
			warns = append(warns, d.Summary)
		}
	}

	return warns, errs
}

func protoDiags(warns []string, errs []error) (diags []*proto.Diagnostic) {
	for _, w := range warns {
		diags = append(diags, &proto.Diagnostic{
			Level:   proto.Diagnostic_WARNING,
			Summary: w,
		})
	}

	for _, e := range errs {
		diags = append(diags, &proto.Diagnostic{
			Level:   proto.Diagnostic_ERROR,
			Summary: e.Error(),
		})
	}
	return diags
}
