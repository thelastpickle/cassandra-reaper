package io.cassandrareaper.resources;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import com.codahale.metrics.health.HealthCheck;
import jakarta.ws.rs.core.Response;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

/**
 * Unit tests for PingResource class. Tests the HEAD and GET ping endpoints with healthy and
 * unhealthy health checks.
 */
public class PingResourceTest {

  private HealthCheck mockHealthCheck;
  private PingResource pingResource;

  @BeforeEach
  public void setUp() {
    mockHealthCheck = mock(HealthCheck.class);
    pingResource = new PingResource(mockHealthCheck);
  }

  @Test
  public void testHeadPingWhenHealthy() {
    // Given: Health check returns healthy result
    HealthCheck.Result healthyResult = HealthCheck.Result.healthy();
    when(mockHealthCheck.execute()).thenReturn(healthyResult);

    // When: HEAD ping is called
    Response response = pingResource.headPing();

    // Then: Should return 204 No Content
    assertThat(response.getStatus()).isEqualTo(Response.Status.NO_CONTENT.getStatusCode());
    verify(mockHealthCheck).execute();
  }

  @Test
  public void testHeadPingWhenUnhealthy() {
    // Given: Health check returns unhealthy result
    HealthCheck.Result unhealthyResult = HealthCheck.Result.unhealthy("Service unavailable");
    when(mockHealthCheck.execute()).thenReturn(unhealthyResult);

    // When: HEAD ping is called
    Response response = pingResource.headPing();

    // Then: Should return 500 Internal Server Error
    assertThat(response.getStatus())
        .isEqualTo(Response.Status.INTERNAL_SERVER_ERROR.getStatusCode());
    verify(mockHealthCheck).execute();
  }

  @Test
  public void testGetPingWhenHealthy() {
    // Given: Health check returns healthy result
    HealthCheck.Result healthyResult = HealthCheck.Result.healthy();
    when(mockHealthCheck.execute()).thenReturn(healthyResult);

    // When: GET ping is called
    Response response = pingResource.getPing();

    // Then: Should return 204 No Content
    assertThat(response.getStatus()).isEqualTo(Response.Status.NO_CONTENT.getStatusCode());
    verify(mockHealthCheck).execute();
  }

  @Test
  public void testGetPingWhenUnhealthy() {
    // Given: Health check returns unhealthy result
    HealthCheck.Result unhealthyResult = HealthCheck.Result.unhealthy("Database connection failed");
    when(mockHealthCheck.execute()).thenReturn(unhealthyResult);

    // When: GET ping is called
    Response response = pingResource.getPing();

    // Then: Should return 500 Internal Server Error
    assertThat(response.getStatus())
        .isEqualTo(Response.Status.INTERNAL_SERVER_ERROR.getStatusCode());
    verify(mockHealthCheck).execute();
  }

  @Test
  public void testConstructorStoresHealthCheck() {
    // Given: A health check instance
    HealthCheck healthCheck = mock(HealthCheck.class);

    // When: PingResource is created
    PingResource resource = new PingResource(healthCheck);

    // Then: The health check should be stored (verified by using it)
    HealthCheck.Result result = HealthCheck.Result.healthy();
    when(healthCheck.execute()).thenReturn(result);

    Response response = resource.headPing();
    assertThat(response.getStatus()).isEqualTo(Response.Status.NO_CONTENT.getStatusCode());
    verify(healthCheck).execute();
  }
}
