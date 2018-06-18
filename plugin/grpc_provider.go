package plugin

import (
	"context"
	"errors"
	"sync"

	"github.com/hashicorp/terraform/config/configschema"
	"github.com/hashicorp/terraform/plugin/proto"
	"github.com/hashicorp/terraform/terraform"
	"github.com/hashicorp/terraform/tfdiags"
	"github.com/hashicorp/terraform/types"
	"github.com/hashicorp/terraform/version"
	"github.com/zclconf/go-cty/cty"
	"github.com/zclconf/go-cty/cty/msgpack"
	"google.golang.org/grpc"
)

var _ types.Provider = (*GRPCProvider)(nil)

// terraform.ResourceProvider grpc implementation
type GRPCProvider struct {
	conn   *grpc.ClientConn
	client proto.ProviderClient

	// this context is created by the plugin package, and is canceled when the
	// plugin process ends.
	ctx context.Context

	// schema stores the schema for this provider. This is used to properly
	// serialize the state for requests.
	mu      sync.Mutex
	schemas *types.GetSchemaResponse
}

// getSchema is used internally to get the saved provider schema.  The schema
// should have already been fetched from the provider, but we have to
// synchronize access to avoid being called concurrently with GetSchema.  This
// method will panic if the schema cannot be found.
func (p *GRPCProvider) getSchema() *configschema.Block {
	p.mu.Lock()
	// unlock inline in case GetSchema needs to be called
	if p.schemas != nil {
		return p.schemas
	}
	p.mu.Unlock()

	// the schema should have been fetched already, but give it another shot
	// just in case things are being called out of order. This may happen for
	// tests.
	schemas, err := p.GetSchema()
	if err != nil {
		panic(err)
	}

	p.schemas = schemas
	return schemas
}

// getResourceSchema is a helper to extract the schema for a resource, and
// panics if the schema is not available.
func (p *GRPCProvider) getResourceSchema(name string) *configschema.Block {
	schema := p.getSchema()
	rSchema := schema.ResourceTypes[name]
	if rSchema == nil {
		panic("unknown resource type " + name)
	}
	return rSchema
}

// gettDatasourceSchema is a helper to extract the schema for a datasource, and
// panics if that schema is not available.
func (p *GRPCProvider) getDatasourceSchema(name string) *configschema.Block {
	schema := p.getSchema()
	dSchema := schema.DataSources[name]
	if dSchema == nil {
		panic("unknown data source " + name)
	}
	return dSchema
}

func (p *GRPCProvider) GetSchema() types.GetSchemaResponse {
	p.mu.Lock()
	defer p.mu.Unlock()

	if p.schemas != nil {
		return p.schemas, nil
	}

	schema := types.GetSchemaResponse{
		Provider:      schemaBlock(resp.ProviderSchema),
		ResourceTypes: make(map[string]*configschema.Block),
		DataSources:   make(map[string]*configschema.Block),
	}

	resp, err := p.client.GetSchema(p.ctx, new(proto.GetSchema_Request))
	if err != nil {
		return nil, err
	}

	for name, res := range resp.ResourceSchemas {
		schema.ResourceTypes[name] = schemaBlock(res)
	}

	for name, data := range resp.DataSourceSchemas {
		schema.ResourceTypes[name] = schemaBlock(data)
	}

	p.schema = schema

	return schema, nil
}

func (p *GRPCProvider) ValidateProviderConfig(val cty.Value) tfdiags.Diagnostics {
	mp, err := msgpack.Marshal(val, p.schema.Provider.ImpliedType())
	if err != nil {
		return tfdiags.Diagnostics.Append(nil, err)
	}

	req := &proto.ValidateProviderConfig_Request{
		Config: &proto.DynamicValue{Msgpack: mp},
	}
	resp, err := p.client.ValidateProviderConfig(p.ctx, req)
	if err != nil {
		return tfdiags.Diagnostics.Append(nil, err)
	}

	return diagnostics(resp.Diagnostics)
}

func (p *GRPCProvider) ValidateResourceTypeConfig(t string, val cty.Value) tfdiags.Diagnostics {
	res := p.getResourceSchema(t)

	mp, err := msgpack.Marshal(val, res.ImpliedType())
	if err != nil {
		return tfdiags.Diagnostics.Append(nil, err)
	}

	req := &proto.ValidateResourceTypeConfig_Request{
		ResourceTypeName: t,
		Config:           &proto.DynamicValue{Msgpack: mp},
	}

	resp, err := p.client.ValidateResourceTypeConfig(p.ctx, req)
	if err != nil {
		return tfdiags.Diagnostics.Append(nil, err)
	}

	return diagnostics(resp.Diagnostics)
}

func (p *GRPCProvider) ValidateDataSourceConfig(t string, config cty.Value) tfdiags.Diagnostics {
	dat := p.getDataousrceSchema(t)

	mp, err := msgpack.Marshal(config, dat.ImpliedType())
	if err != nil {
		return tfdiags.Diagnostics.Append(nil, err)
	}

	req := &proto.ValidateDataSourceConfig_Request{
		DataSourceName: t,
		Config:         &proto.DynamicValue{Msgpack: mp},
	}

	resp, err := p.client.ValidateDataSourceConfig(p.ctx, req)
	if err != nil {
		return tfdiags.Diagnostics.Append(nil, err)
	}

	return diagnostics(resp.Diagnostics)

}

func (p *GRPCProvider) UpgradeResourceState(name string, version int, state cty.Value) (cty.Value, tfdiags.Diagnostics) {
	res := p.getResourceSchema(name)

	// TODO: how to encode raw state?
	mp, err := msgpack.Marshal(state, res.ImpliedType())
	if err != nil {
		return cty.NilVal, tfdiags.Diagnostics.Append(nil, err)
	}

	req := &proto.UpgradeResourceState_Request{
		ResourceTypeName: name,
		PriorVersion:     uint64(version),
		PriorStateRaw: &proto.DynamicValue{
			Msgpack: mp,
		},
	}

	resp, err := p.client.UpgradeResourceState(p.ctx, req)
	if err != nil {
		return cty.NilVal, tfdiags.Diagnostics.Append(nil, err)
	}

	val, err := msgpack.Unmarshal(resp.NewState.Msgpack, res.ImpliedType())
	if err != nil {
		return cty.NilVal, tfdiags.Diagnostics.Append(nil, err)
	}

	return val, diagnostics(resp.Diagnostics)
}

func (p *GRPCProvider) Configure(val cty.Value) tfdiags.Diagnostics {
	schema := p.getSchema()

	dv, err := msgpack.Marshal(val, schema.Provider.ImpliedType())
	if err != nil {
		return tfdiags.Diagnostics.Append(nil, err)
	}

	req := &proto.Configure_Request{
		TerraformVersion: version.Version,
		Config: &proto.DynamicValue{
			Msgpack: dv,
		},
	}

	resp, err := p.client.Configure(p.ctx, req)
	if err != nil {
		return tfdiags.Diagnostics.Append(nil, err)
	}

	return diagnostics(resp.Diagnostics)
}

func (p *GRPCProvider) Stop() error {
	resp, err := p.client.Stop(p.ctx, new(proto.Stop_Request))
	if err != nil {
		return err
	}

	if resp.Error != "" {
		return errors.New(resp.Error)
	}
	return nil
}

func (p *GRPCProvider) ReadResource(name string, prior cty.Value) (cty.Value, tfdiags.Diagnostics) {
	schema, err := p.GetSchema()
	if err != nil {
		panic(err)
	}

	_ = schema
	return cty.NilVal, nil
}

func (p *GRPCProvider) PlanResourceChange(name string, prior, proposed, private cty.Value) (cty.Value, []byte, tfdiags.Diagnostics) {
	res := p.getResourceSchema(name)

	priorMP, err := msgpack.Marshal(prior, res.ImpliedType())
	if err != nil {
		return cty.NilVal, tfdiags.Diagnostics.Append(nil, err)
	}
	propMP, err := msgpack.Marshal(proposed, res.ImpliedType())
	if err != nil {
		return cty.NilVal, tfdiags.Diagnostics.Append(nil, err)
	}

	req := &proto.PlanResourceChange_Request{
		PriorState: &DynamicValue{Msgpack: priorMP},
		ProposedNewState & DynamicValue{Msgpack: propMP},
		Private: private,
	}

	resp, err := p.client.PlanResourceChange(req)
	if err != nil {
		return cty.NilValue, tfdiags.Diagnostics.Append(nil, err)
	}

	return cty.NilVal, nil
}

func (p *GRPCProvider) ApplyResourceChange(name string, prior, planned cty.Value) (cty.Value, tfdiags.Diagnostics) {
	schema, err := p.GetSchema()
	if err != nil {
		panic(err)
	}

	panic("unimplemented")
	_ = schema
	return cty.NilVal, nil
}

func (p *GRPCProvider) ImportResourceState(name, id string) ([]cty.Value, tfdiags.Diagnostics) {
	schema, err := p.GetSchema()
	if err != nil {
		panic(err)
	}

	panic("unimplemented")
	_ = schema
	return nil, nil
}

func (p *GRPCProvider) ReadDataSource(name string) (cty.Value, tfdiags.Diagnostics) {
	schema, err := p.GetSchema()
	if err != nil {
		panic(err)
	}

	panic("unimplemented")
	_ = schema
	return cty.NilVal, nil
}

// closing the grpc connection is final, and terraform will call it at the end of every phase.
func (p *GRPCProvider) Close() error {
	return nil
}

type GRPCProviderServer struct {
	provider terraform.ResourceProvider
}

func (s *GRPCProviderServer) GetSchema(_ context.Context, req *proto.GetSchema_Request) (*proto.GetSchema_Response, error) {
	// GetSchema must return the full schema
	ps, err := s.provider.GetSchema(nil)
	if err != nil {
		// FIXMEa diags
		return nil, err
	}

	resp := &proto.GetSchema_Response{
		ProviderSchema:    protoSchemaBlock(ps.Provider),
		ResourceSchemas:   make(map[string]*proto.GetSchema_Block),
		DataSourceSchemas: make(map[string]*proto.GetSchema_Block),
	}

	for name, r := range ps.ResourceTypes {
		resp.ResourceSchemas[name] = protoSchemaBlock(r)
	}
	for name, r := range ps.DataSources {
		resp.DataSourceSchemas[name] = protoSchemaBlock(r)
	}

	return resp, nil
}

func (s *GRPCProviderServer) ValidateProviderConfig(_ context.Context, req *proto.ValidateProviderConfig_Request) (*proto.ValidateProviderConfig_Response, error) {
	ps, err := s.provider.GetSchema(nil)
	if err != nil {
		// FIXME
		return nil, err
	}

	val, err := msgpack.Unmarshal(req.Config.Msgpack, ps.Provider.ImpliedType())
	if err != nil {
		// FIXME
		return nil, err
	}

	panic("conversion not implemented")
	_ = val
	warns, errs := s.provider.Validate(nil)

	return &proto.ValidateProviderConfig_Response{Diagnostics: protoDiags(warns, errs)}, nil
}

func (s *GRPCProviderServer) ValidateResourceTypeConfig(_ context.Context, req *proto.ValidateResourceTypeConfig_Request) (*proto.ValidateResourceTypeConfig_Response, error) {
	ps, err := s.provider.GetSchema(nil)
	if err != nil {
		// FIXME
		return nil, err
	}

	schema := ps.ResourceTypes[req.ResourceTypeName]
	if schema == nil {
		// FIXME
		return nil, nil
	}

	cfg, err := msgpack.Unmarshal(req.Config.Msgpack, schema.ImpliedType())
	panic("conversion not implemented")
	_ = cfg

	w, e := s.provider.ValidateResource(req.ResourceTypeName, nil)
	return &proto.ValidateResourceTypeConfig_Response{Diagnostics: protoDiags(w, e)}, nil
}

func (s *GRPCProviderServer) ValidateDataSourceConfig(_ context.Context, req *proto.ValidateDataSourceConfig_Request) (*proto.ValidateDataSourceConfig_Response, error) {
	ps, err := s.provider.GetSchema(nil)
	if err != nil {
		// FIXME
		return nil, err
	}

	schema := ps.DataSources[req.DataSourceName]
	if schema == nil {
		// FIXME
		return nil, nil
	}

	panic("conversion not implemented")
	return nil, nil
}

func (s *GRPCProviderServer) UpgradeResourceState(_ context.Context, _ *proto.UpgradeResourceState_Request) (*proto.UpgradeResourceState_Response, error) {
	return nil, nil
}

func (s *GRPCProviderServer) Stop(_ context.Context, _ *proto.Stop_Request) (*proto.Stop_Response, error) {
	resp := &proto.Stop_Response{}

	err := s.provider.Stop()
	if err != nil {
		resp.Error = err.Error()
	}

	return resp, nil
}
func (s *GRPCProviderServer) Configure(_ context.Context, req *proto.Configure_Request) (*proto.Configure_Response, error) {
	ps, err := s.provider.GetSchema(nil)
	if err != nil {
		// FIXME
		return nil, err
	}

	cfg, err := msgpack.Unmarshal(req.Config.Msgpack, ps.Provider.ImpliedType())
	if err != nil {
		return nil, err
	}

	panic("conversion not implemented")
	_ = cfg
	err = s.provider.Configure(nil)
	return nil, err
}

func (s *GRPCProviderServer) ReadResource(_ context.Context, req *proto.ReadResource_Request) (*proto.ReadResource_Response, error) {
	ps, err := s.provider.GetSchema(nil)
	if err != nil {
		// FIXME
		return nil, err
	}

	_ = ps
	panic("conversion not implemented")

	_, err = s.provider.Refresh(nil, nil)
	if err != nil {
		return nil, err
	}

	return nil, nil
}

func (s *GRPCProviderServer) PlanResourceChange(_ context.Context, req *proto.PlanResourceChange_Request) (*proto.PlanResourceChange_Response, error) {
	ps, err := s.provider.GetSchema(nil)
	if err != nil {
		// FIXME
		return nil, err
	}

	_ = ps
	panic("conversion not implemented")

	info := &terraform.InstanceInfo{}
	state := &terraform.InstanceState{}
	cfg := &terraform.ResourceConfig{}

	info.Type = req.ResourceTypeName

	_, err = s.provider.Diff(info, state, cfg)
	if err != nil {
		return nil, err
	}

	return nil, nil
}

func (s *GRPCProviderServer) ApplyResourceChange(_ context.Context, req *proto.ApplyResourceChange_Request) (*proto.ApplyResourceChange_Response, error) {
	ps, err := s.provider.GetSchema(nil)
	if err != nil {
		// FIXME
		return nil, err
	}

	_ = ps
	panic("conversion not implemented")

	info := &terraform.InstanceInfo{}
	state := &terraform.InstanceState{}
	diff := &terraform.InstanceDiff{}

	info.Type = req.ResourceTypeName

	_, err = s.provider.Apply(info, state, diff)
	if err != nil {
		return nil, err
	}

	return nil, nil
}

func (s *GRPCProviderServer) ImportResourceState(_ context.Context, req *proto.ImportResourceState_Request) (*proto.ImportResourceState_Response, error) {
	ps, err := s.provider.GetSchema(nil)
	if err != nil {
		// FIXME
		return nil, err
	}

	_ = ps
	panic("conversion not implemented")

	info := &terraform.InstanceInfo{}
	info.Type = req.ResourceTypeName

	_, err = s.provider.ImportState(info, req.Id)
	if err != nil {
		return nil, err
	}

	return nil, nil
}

func (s *GRPCProviderServer) ReadDataSource(_ context.Context, req *proto.ReadDataSource_Request) (*proto.ReadDataSource_Response, error) {
	ps, err := s.provider.GetSchema(nil)
	if err != nil {
		// FIXME
		return nil, err
	}

	_ = ps
	panic("conversion not implemented")
	return nil, nil
}
