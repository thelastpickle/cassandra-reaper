/**
 * Utility method for digging through the endpointStates API response structure
 * to identify the possible nodes
 *
 * @returns An array of node/endpoint objects
 */
import {node} from "prop-types";

export const getNodesFromEndpointStates = function(endpointStates) {
  const nodes = [];
  if (!endpointStates || !endpointStates.length) {
    return nodes;
  }

  for(let endpointState of endpointStates) {
    if (!endpointState || !endpointState.endpoints) {
      continue;
    }
    for (let datacenterId in endpointState.endpoints) {
      const datacenter = endpointState.endpoints[datacenterId];
      if (!datacenter) {
        continue;
      }
      for (let rackId in datacenter) {
        const rack = datacenter[rackId];
        if (!rack) {
          continue;
        }
        for (let endpoint of rack) {
          if (endpoint) {
            nodes.push(endpoint);
          }
        }
      }
    }
  }
  return nodes;
}

/**
 * Utility function for generating a set of options for a select representing a drop down of nodes.
 * If excludeStargateNodes is true stargate nodes will not be included in the set of options generated.
 * @param endpointStates
 * @param excludeStargateNodes
 * @returns {{nodeOptions: {label: *, value: *}[]}|{nodeOptions: *[]}}
 */
export const getNodeOptions = function(endpointStates, excludeStargateNodes) {
  let nodes = getNodesFromEndpointStates(endpointStates);
  if (!nodes) {
    return {
      nodeOptions: []
    }
  }
  const includedNodes = excludeStargateNodes ? nodes.filter(node => node.type !== "STARGATE") : nodes;
  return {
    nodeOptions: includedNodes.map(node => node.endpoint).sort().map(
      obj => { return {value: obj, label: obj}; }
    )
  };
}