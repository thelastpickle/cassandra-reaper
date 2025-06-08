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
  sessionTimeout: PT10M            # Session timeout (ISO 8601 duration)
  jwt:
    secret: "your-jwt-secret-key"  # JWT signing secret (minimum 256 bits for HS256)
    tokenExpirationTime: PT10M     # JWT token expiration (defaults to sessionTimeout)
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
- **Description**: Session timeout for WebUI authentication

#### `jwt.secret`
- **Type**: String
- **Required**: Yes (when JWT is used)
- **Description**: Secret key for signing JWT tokens. Must be at least 256 bits (32 characters) for HS256 algorithm
- **Security**: Use a cryptographically strong random string in production

#### `jwt.tokenExpirationTime`
- **Type**: ISO 8601 Duration
- **Default**: Same as `sessionTimeout`
- **Description**: JWT token expiration time

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
    tokenExpirationTime: "${JWT_TOKEN_EXPIRATION:-PT10M}"
  users:
    - username: "${REAPER_AUTH_USER:-admin}"
      password: "${REAPER_AUTH_PASSWORD:-admin}"
      roles: ["operator"]
    - username: "${REAPER_READ_USER:-user}"
      password: "${REAPER_READ_USER_PASSWORD:-user}"
      roles: ["user"]
```

## Docker Configuration

### Environment Variables

When running Reaper in Docker, set these environment variables:

```bash
# Authentication control
REAPER_AUTH_ENABLED=true

# JWT Configuration
JWT_SECRET="your-production-jwt-secret-key-here"
JWT_TOKEN_EXPIRATION="PT1H"

# Admin user credentials - REQUIRED when authentication is enabled
REAPER_AUTH_USER="admin"
REAPER_AUTH_PASSWORD="your-secure-admin-password-here"

# Read-only user credentials - OPTIONAL
REAPER_READ_USER="monitoring"
REAPER_READ_USER_PASSWORD="your-secure-monitoring-password-here"

# Session timeout
REAPER_SESSION_TIMEOUT="PT30M"
```

> **⚠️ Important**: You **must** set `REAPER_AUTH_PASSWORD` and any other password environment variables, or Reaper will fail to start. Never leave passwords empty or use default values.

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
      JWT_TOKEN_EXPIRATION: "PT1H"
      
             # User credentials - CHANGE THESE!
       REAPER_AUTH_USER: "admin"
       REAPER_AUTH_PASSWORD: "change-this-secure-password"
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

### Kubernetes Deployment

```yaml
apiVersion: v1
kind: Secret
metadata:
  name: reaper-auth-secret
type: Opaque
stringData:
  jwt-secret: "your-production-jwt-secret-key"
  admin-password: "your-secure-admin-password" 
  user-password: "your-secure-monitoring-password"

---
apiVersion: apps/v1
kind: Deployment
metadata:
  name: cassandra-reaper
spec:
  template:
    spec:
      containers:
      - name: reaper
        image: cassandra-reaper:latest
        env:
        - name: REAPER_AUTH_ENABLED
          value: "true"
        - name: JWT_SECRET
          valueFrom:
            secretKeyRef:
              name: reaper-auth-secret
              key: jwt-secret
        - name: REAPER_AUTH_PASSWORD
          valueFrom:
            secretKeyRef:
              name: reaper-auth-secret
              key: admin-password
        - name: REAPER_READ_USER_PASSWORD
          valueFrom:
            secretKeyRef:
              name: reaper-auth-secret
              key: user-password
```

## API Authentication

### REST API Access

The REST API supports both JWT and Basic Authentication:

#### JWT Authentication (Recommended)

1. **Login to get JWT token**:
   ```bash
   curl -X POST http://localhost:8080/login \
     -H "Content-Type: application/x-www-form-urlencoded" \
     -d "username=admin&password=admin"
   ```

   Response:
   ```json
   {
     "token": "eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9...",
     "user": "admin",
     "roles": ["operator"]
   }
   ```

2. **Use JWT token in API calls**:
   ```bash
   curl -H "Authorization: Bearer eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9..." \
     http://localhost:8080/cluster
   ```

#### Basic Authentication

```bash
curl -u admin:admin http://localhost:8080/cluster
```

### Command Line Tool (`spreaper`)

When authentication is enabled, use the login command:

```bash
# Login and save JWT token
./spreaper login admin
Password: *****
# Logging in...
You are now authenticated to Reaper.
# JWT saved

# Use saved token for subsequent commands
./spreaper cluster list

# Or provide token directly
./spreaper --jwt <token> cluster list
```

The JWT token is saved in `~/.reaper/jwt` and automatically used for subsequent calls.

## Web UI Authentication

The web UI is protected by a servlet filter that:

1. **Redirects unauthenticated users** to `/webui/login.html`
2. **Validates JWT tokens** from:
   - `Authorization: Bearer <token>` header (for AJAX requests)
   - `jwtToken` cookie (for browser navigation)
3. **Allows unrestricted access** to:
   - Login page (`/webui/login.html`)
   - Static assets (CSS, JS, images)

### Login Process

1. Navigate to `http://localhost:8080/webui/`
2. You'll be redirected to the login page
3. Enter your credentials
4. Upon successful login, you'll receive a JWT token
5. The token is stored in browser session storage
6. All subsequent requests include the token

## Security Considerations

### Production Deployment

1. **Change default credentials** immediately
2. **Use strong JWT secret** (minimum 256 bits, cryptographically random)
3. **Use environment variables** for sensitive configuration
4. **Enable HTTPS** for production deployments
5. **Set appropriate token expiration** times
6. **Regularly rotate JWT secrets** and passwords

### JWT Secret Generation

Generate a secure JWT secret:

```bash
# Generate 32-byte (256-bit) random secret
openssl rand -base64 32

# Or use a UUID-based approach
uuidgen | tr -d '-' | head -c 32
```

### Password Security

Currently, passwords are stored in plain text in the configuration. For enhanced security:

1. Use strong, unique passwords
2. Limit access to configuration files
3. Use environment variables in containerized deployments
4. Consider external secret management systems

## Troubleshooting

### Common Issues

1. **Authentication disabled but still prompted for login**:
   - Ensure `accessControl.enabled: false` in configuration
   - Restart Reaper after configuration changes

2. **JWT token expired**:
   - Re-login to get a new token
   - Check `tokenExpirationTime` configuration

3. **Invalid JWT secret**:
   - Ensure secret is at least 32 characters long
   - Use the same secret across all Reaper instances

4. **Docker environment variables not working**:
   - Verify environment variable names match exactly
   - Use quotes around values containing special characters

### Debug Logging

Enable debug logging for authentication:

```yaml
logging:
  level: INFO
  loggers:
    io.cassandrareaper.auth: DEBUG
```

This will log authentication attempts, token validation, and authorization decisions.

## Migration from Shiro

If you're migrating from a Shiro-based authentication setup:

1. **Remove Shiro configuration** from your YAML file
2. **Replace with Dropwizard `accessControl` section**
3. **Update user definitions** to use the new format
4. **Replace `shiro.ini` file references** with inline user configuration
5. **Update any custom authentication integrations** to use JWT/Basic Auth

The new system provides equivalent functionality with modern standards and better Docker/Kubernetes integration.
