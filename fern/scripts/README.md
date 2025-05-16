# Airweave API SDK and Documentation Generation

This directory contains scripts for generating the OpenAPI specification for Airweave's API, which is then used to generate SDKs and documentation via Fern.

## API Filtering

The OpenAPI specification is filtered to only include specific API endpoints for the SDK and public documentation. This is controlled by the configuration in `api_config.py`.

### How it works

1. `generate_openapi.py` generates the full OpenAPI spec from the FastAPI app
2. It then filters the spec to only include endpoints defined in `api_config.py`
3. The filtered spec is saved to `../definition/openapi.json`
4. Fern uses this spec to generate SDKs and documentation

### Modifying included endpoints

To modify which endpoints are included in the SDK and documentation:

1. Edit `api_config.py` and modify the `INCLUDED_ENDPOINTS` dictionary
2. Add or remove entries as needed, following the format:
   ```python
   "/path/to/endpoint/": {"http_method": True}
   ```
3. Run `python fern/scripts/generate_openapi.py` to regenerate the OpenAPI spec
4. Run the Fern generators to update the SDK and documentation

### API Groups

The current API is limited to four main groups:
- Sources
- Collections
- Source Connections
- White Labels

Each group has specific endpoints that are included in the SDK and documentation.
