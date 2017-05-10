package com.spotify.reaper.core;

public final class HostMetrics {
  private final String hostAddress;
  private final int pendingCompactions;
  private final boolean hasRepairRunning;
  private final int activeAnticompactions;


  private HostMetrics(Builder builder) {
  	this.hostAddress = builder.hostAddress;
  	this.pendingCompactions = builder.pendingCompactions;
  	this.hasRepairRunning = builder.hasRepairRunning;
  	this.activeAnticompactions = builder.activeAnticompactions;
  }
  
  public String getHostAddress() {
    return hostAddress;
  }

  public int getPendingCompactions() {
    return pendingCompactions;
  }

  public boolean hasRepairRunning() {
    return hasRepairRunning;
  }

  public int getActiveAnticompactions() {
    return activeAnticompactions;
  }


  /**
   * Creates builder to build {@link HostMetrics}.
   * @return created builder
   */
  public static Builder builder() {
  	return new Builder();
  }

  /**
   * Builder to build {@link HostMetrics}.
   */
  public static final class Builder {
  	private String hostAddress;
  	private int pendingCompactions;
  	private boolean hasRepairRunning;
  	private int activeAnticompactions;
  
  	private Builder() {
  	}
  
  	public Builder withHostAddress(String hostAddress) {
  		this.hostAddress = hostAddress;
  		return this;
  	}
  
  	public Builder withPendingCompactions(int pendingCompactions) {
  		this.pendingCompactions = pendingCompactions;
  		return this;
  	}
  
  	public Builder withHasRepairRunning(boolean hasRepairRunning) {
  		this.hasRepairRunning = hasRepairRunning;
  		return this;
  	}
  
  	public Builder withActiveAnticompactions(int activeAnticompactions) {
  		this.activeAnticompactions = activeAnticompactions;
  		return this;
  	}
  
  	public HostMetrics build() {
  		return new HostMetrics(this);
  	}
  }
}
