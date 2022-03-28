//  Licensed under the Apache License, Version 2.0 (the "License");
//  you may not use this file except in compliance with the License.
//  You may obtain a copy of the License at
//
//      http://www.apache.org/licenses/LICENSE-2.0
//
//  Unless required by applicable law or agreed to in writing, software
//  distributed under the License is distributed on an "AS IS" BASIS,
//  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
//  See the License for the specific language governing permissions and
//  limitations under the License.

/**
 * Utility method for digging through the endpointStates API response structure
 * to identify the possible nodes
 *
 * @param endpointStates - Array
 * @returns An array of node/endpoint objects
 */

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
 *
 * @param endpointStates - Array
 * @param excludeStargateNodes - Boolean
 * @returns An object suitable for passing to Select component
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
