+++
[menu.docs]
name = "Authentication"
weight = 60
identifier = "auth"
parent = "configuration"
+++

# Authentication

Authentication in Reaper is powered by [Dropwizard Authentication](https://www.dropwizard.io/en/latest/manual/auth.html) with JWT (JSON Web Token) support. This provides a modern, stateless authentication system suitable for both web UI and REST API access.

## Overview

Reaper implements a dual authentication strategy:
- **JWT Authentication**: Used for REST API endpoints and modern web applications
- **Basic Authentication**: Available as a fallback for simple integrations
- **WebUI Protection**: Custom servlet filter that protects the web interface

Authentication is **enabled by default** but **requires explicit user configuration**. No default users are provided for security reasons - you must configure at least one user or authentication will fail to start.

## Configuration

Authentication is configured in the `accessControl` section of your YAML configuration:

```yaml
accessControl:
  enabled: true                    # Enable/disable authentication
  sessionTimeout: PT10M            # Session/token timeout (ISO 8601 duration)
  jwt:
    secret: "your-jwt-secret-key"  # JWT signing secret (minimum 256 bits for HS256)
  users:
    - username: "admin"            # REQUIRED: Must not be empty
      password: "secure-password"  # REQUIRED: Must not be empty
      roles: ["operator"]          # REQUIRED: Must have at least one role
    - username: "monitoring"
      password: "another-secure-password"
      roles: ["user"]
```

> **⚠️ Security Notice**: You **must** configure at least one user with a non-empty password, or Reaper will fail to start. Never use default or weak passwords in production.

### Configuration Parameters

#### `enabled`
- **Type**: Boolean
- **Default**: `true`
- **Description**: Enables or disables authentication globally

#### `sessionTimeout`
- **Type**: ISO 8601 Duration
- **Default**: `PT10M` (10 minutes)
- **Description**: Session timeout that applies to both WebUI authentication and JWT token expiration. This is the primary timeout setting.

#### `jwt.secret`
- **Type**: String
- **Required**: Yes (when JWT is used)
- **Description**: Secret key for signing JWT tokens. Must be at least 256 bits (32 characters) for HS256 algorithm
- **Security**: Use a cryptographically strong random string in production

#### `users`
- **Type**: Array of user objects
- **Description**: In-memory user store configuration

#### User Object Properties
- `username`: String - User login name (required, cannot be empty)
- `password`: String - User password (required, cannot be empty, stored in plain text)  
- `roles`: Array of strings - User roles (required, must contain at least one role: `["user", "operator"]`)

#### User Validation
Reaper validates all user configurations on startup and will **fail to start** if:
- No users are configured when authentication is enabled
- Any user has an empty or missing username
- Any user has an empty or missing password
- Any user has no roles assigned

This ensures that weak or incomplete authentication configurations are caught early.

## Conditional User Configuration

Reaper supports conditional configuration of additional users through environment variables and Docker configuration scripts. This approach allows you to:

- **Always configure the primary admin user** via `REAPER_AUTH_USER` and `REAPER_AUTH_PASSWORD` (required)
- **Optionally configure a read-only user** by setting both `REAPER_READ_USER` and `REAPER_READ_USER_PASSWORD` environment variables

### How It Works

The read-only user is configured dynamically at container startup:

1. If both `REAPER_READ_USER` and `REAPER_READ_USER_PASSWORD` are set to non-empty values, a read-only user with the `["user"]` role is automatically added to the configuration
2. If either variable is empty or unset, no read-only user is configured
3. This prevents environment variable resolution errors while maintaining security

### Benefits

- **No mandatory unused environment variables**: You don't need to set empty values for unused users
- **Flexible deployment**: Same Docker image can be used with or without read-only users
- **Security by default**: Empty or missing credentials don't create weak authentication points

## User Roles

Reaper implements role-based access control with two main roles:

### `user` Role
- **Read-only access** to all resources
- Can view clusters, repair runs, schedules, and metrics
- Cannot create, modify, or delete resources
- Suitable for monitoring and reporting users

### `operator` Role
- **Full access** to all resources
- Can create, modify, and delete clusters, repair runs, and schedules
- Can trigger repairs and manage all Reaper functionality
- Suitable for administrators and operational users

## Environment Variable Configuration

For production deployments, especially with Docker, use environment variables instead of hardcoded values:

```yaml
accessControl:
  enabled: ${REAPER_AUTH_ENABLED:-true}
  sessionTimeout: ${REAPER_SESSION_TIMEOUT:-PT10M}
  jwt:
    secret: "${JWT_SECRET:-MySecretKeyForJWTWhichMustBeLongEnoughForHS256Algorithm}"
  users:
    - username: "${REAPER_AUTH_USER}"
      password: "${REAPER_AUTH_PASSWORD}"
      roles: ["operator"]
    # Additional read-only user is configured automatically if REAPER_READ_USER
    # and REAPER_READ_USER_PASSWORD environment variables are set
```

## Docker Configuration

### Environment Variables

When running Reaper in Docker, set these environment variables:

```bash
# Authentication control
REAPER_AUTH_ENABLED=true

# JWT Configuration
JWT_SECRET="your-production-jwt-secret-key-here"

# Admin user credentials - REQUIRED when authentication is enabled
REAPER_AUTH_USER="admin"
REAPER_AUTH_PASSWORD="your-secure-admin-password-here"

# Read-only user credentials - OPTIONAL (only configured if both are set)
REAPER_READ_USER="monitoring"
REAPER_READ_USER_PASSWORD="your-secure-monitoring-password-here"

# Session timeout
REAPER_SESSION_TIMEOUT="PT30M"
```

> **⚠️ Important**: You **must** set `REAPER_AUTH_USER` and `REAPER_AUTH_PASSWORD`, or Reaper will fail to start. The read-only user (`REAPER_READ_USER` and `REAPER_READ_USER_PASSWORD`) is optional - it will only be configured if both environment variables are set to non-empty values.

### Docker Compose Example

```yaml
version: '3.8'

services:
  cassandra-reaper:
    image: cassandra-reaper:latest
    ports:
      - "8080:8080"
      - "8081:8081"
    environment:
      # Authentication
      REAPER_AUTH_ENABLED: "true"
      JWT_SECRET: "MyProductionJWTSecretKeyThatIsLongEnoughForHS256"
      
                   # User credentials - CHANGE THESE!
      REAPER_AUTH_USER: "admin"
      REAPER_AUTH_PASSWORD: "change-this-secure-password"
      
      # Optional read-only user (remove these lines to disable)
      REAPER_READ_USER: "monitoring"
      REAPER_READ_USER_PASSWORD: "change-this-monitoring-password"
      
      # Session configuration
      REAPER_SESSION_TIMEOUT: "PT30M"
      
      # Storage configuration
      REAPER_STORAGE_TYPE: "cassandra"
      REAPER_CASS_CONTACT_POINTS: "[\"cassandra:9042\"]"
      
    depends_on:
      - cassandra
```