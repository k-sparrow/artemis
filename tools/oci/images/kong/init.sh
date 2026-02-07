# An initialization script for Kong API gateway
# Manipulates Kong services and routes via its Admin API 
# at port 8001

# add the backend users and auth service
# Creates the following URL for frontend access:
# ${KONG_CP_API_GATEWAY}/auth
#
# All functions and endpoints are available under this URL
curl -i -X POST ${KONG_CP_ADMIN_API}/services --data name=Authentication --data url=${BACKEND_AUTH_URL}
curl -i -X POST ${KONG_CP_ADMIN_API}/services/Authentication/routes --data "paths[]=/auth" --data name=auth_route

# add the storage service
# Creates the following URL for frontend access:
# ${KONG_CP_API_GATEWAY}/storage
#
# All functions and endpoints are available under this URL
curl -i -X POST ${KONG_CP_ADMIN_API}/services --data name=Storage --data url=${BACKEND_INDEXING_STORAGE_URL}
curl -i -X POST ${KONG_CP_ADMIN_API}/services/Storage/routes --data "paths[]=/storage" --data name=storage_route


# add a JWT auth plugin on top of storage
# NOTE: Putting a JWT plugin per service is not ideal and will require instantiating N replicas of the same plugin for N different services
#       Instead, figure out how to create a plugin via ${KONG_CP_ADMIN_API}/plugins and add all the services IDs to it (besides Auth)
curl -i -X POST ${KONG_CP_ADMIN_API}/services/Storage/plugins \
  --header "Content-Type: application/json" \
  --header "Accept: application/json" \
  --data '
  {
    "name": "jwt",
    "instance_name": "jwt-auth-plugin-storage",
    "config": {
        "header_names": [
            "authorization"
        ],
        "key_claim_name": "iss",
        "claims_to_verify": []
    }
  }'

# add the catalog service
# Creates the following URL for frontend access:
# ${KONG_CP_API_GATEWAY}/catalog
#
# All functions and endpoints are available under this URL
curl -i -X POST ${KONG_CP_ADMIN_API}/services --data name=Catalog --data url=${BACKEND_CATALOG_URL}
curl -i -X POST ${KONG_CP_ADMIN_API}/services/Catalog/routes --data "paths[]=/catalog" --data name=catalog_route

# add a JWT auth plugin on top of catalog service
# NOTE: Putting a JWT plugin per service is not ideal and will require instantiating N replicas of the same plugin for N different services
#       Instead, figure out how to create a plugin via ${KONG_CP_ADMIN_API}/plugins and add all the services IDs to it (besides Auth)
curl -i -X POST ${KONG_CP_ADMIN_API}/services/Catalog/plugins \
  --header "Content-Type: application/json" \
  --header "Accept: application/json" \
  --data '
  {
    "name": "jwt",
    "instance_name": "jwt-auth-plugin-catalog",
    "config": {
        "header_names": [
            "authorization"
        ],
        "key_claim_name": "iss",
        "claims_to_verify": []
    }
  }'

# add the chat service
# Creates the following URL for frontend access:
# ${KONG_CP_API_GATEWAY}/chat
#
# All functions and endpoints are available under this URL
curl -i -X POST ${KONG_CP_ADMIN_API}/services --data name=Chat --data url=${BACKEND_CHAT_URL}
curl -i -X POST ${KONG_CP_ADMIN_API}/services/Chat/routes --data "paths[]=/chat" --data name=chat_route

# add a JWT auth plugin on top of chat service
# NOTE: Putting a JWT plugin per service is not ideal and will require instantiating N replicas of the same plugin for N different services
#       Instead, figure out how to create a plugin via ${KONG_CP_ADMIN_API}/plugins and add all the services IDs to it (besides Auth)
curl -i -X POST ${KONG_CP_ADMIN_API}/services/Chat/plugins \
  --header "Content-Type: application/json" \
  --header "Accept: application/json" \
  --data '
  {
    "name": "jwt",
    "instance_name": "jwt-auth-plugin-chat",
    "config": {
        "header_names": [
            "authorization"
        ],
        "key_claim_name": "iss",
        "claims_to_verify": []
    }
  }'

consumer_id="jwtissuer"
# Create a consumer for the Chats service
curl -i -X POST ${KONG_CP_ADMIN_API}/consumers \
  --header 'Content-Type: application/json' \
  --header 'Accept: application/json' \
  --data '{
    "username": "'"${consumer_id}"'",
    "custom_id": null,
    "tags": ["auth"]
  }'

# Give the JWT credentials to the consumer
#
# Note: here 'key' corresponds to "iss" in both Kong's JWT plugin and the authorization service
curl -i -X POST ${KONG_CP_ADMIN_API}/consumers/${consumer_id}/jwt \
  --header 'Content-Type: application/json' \
  --header 'Accept: application/json' \
  --data '{
    "algorithm": "'"${JWT_ALGORITHM}"'",
    "key": "'"${JWT_ISS}"'",
    "secret": "'"${JWT_SECRET}"'"
  }'