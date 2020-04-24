// let myGraph = {nodes:[{id:"n1", label:"Alice"}, {id:"n2", label:"Rabbit"}],
//   edges:[{id:"e1",source:"n1",target:"n2",label:"SEES"}]};
// this.setState({'content':
//   <Sigma graph={myGraph} settings={{drawEdges: true, clone: false}}>
//     <RelativeSize initialSize={15}/>
//     <RandomizeNodePositions/>
//   </Sigma>
// })
// const elements = [
//   { data: { id: 'one', "parent": "three", label: 'Stage1-map' } },
//   { data: { id: 'two', "parent": "three", label: 'Stage2-reduce' } },
//   { data: { id: 'three', label: 'Pipeline 1' } },
//   { data: { id: 'four', "parent": "six", label: 'Stage3-map' } },
//   { data: { id: 'five', "parent": "six", label: 'Stage4-reduce' } },
//   { data: { id: 'six', label: 'Pipeline 2' } },
//   { data: { id: 'seven', "parent": "nine", label: 'Stage5-map' } },
//   { data: { id: 'eight', "parent": "nine", label: 'Stage6-reduce' } },
//   { data: { id: 'nine', label: 'Pipeline 3' } },
//   { data: { source: 'one', target: 'two' } },
//   { data: { source: 'two', target: 'seven' } },
//   { data: { source: 'four', target: 'five' } },
//   { data: { source: 'five', target: 'seven' } },
//   { data: { source: 'seven', target: 'eight' } }
// ];


import {defaultSettings} from "./dag-settings";
import ReactDAG, { DAG, DefaultNode, NodeType1, NodeType2, NodeType3 } from "react-dag";
import {Sigma, RandomizeNodePositions, RelativeSize} from 'react-sigma';
import React from "react";

const typeToComponentMap = {
  action: NodeType2,
  condition: NodeType3,
  sink: NodeType1,
  source: DefaultNode,
  transform: NodeType1,
};

const getComponent = (type) =>
  typeToComponentMap[type] ? typeToComponentMap[type] : DefaultNode;

const data = {
  nodes: [
    [{
      id: "1",
      config: {
        label: "Source Node",
        type: "source",
        style: {
          top: "223px",
          left: "52.5px",
        },
      },
    }, 1],
    [{
      id: "2",
      config: {
        label: "Sink Node",
        type: "sink",
        style: {
          top: "261px",
          left: "1077.5px",
        },
      },
    }, 2],
    [{
      id: "3",
      config: {
        label: "Transform Node 3",
        type: "transform",
        style: {
          top: "375px",
          left: "462.5px",
        },
      },
    }, 3],
    [{
      id: "3.5",
      config: {
        label: "Transform Node 3.5",
        type: "transform",
        style: {
          top: "489px",
          left: "667.5px",
        },
      },
    }, 3.5],
    [{
      id: "4",
      config: {
        label: "Transform Node 4",
        type: "transform",
        style: {
          top: "109px",
          left: "667.5px",
        },
      },
    }, 4],
    [{
      id: "4.5",
      config: {
        label: "Transform Node 4.5",
        type: "transform",
        style: {
          top: "33px",
          left: "462.5px",
        },
      },
    }, 4.5],
    [{
      id: "5",
      config: {
        label: "Transform Node 5",
        type: "transform",
        style: {
          top: "185px",
          left: "872.5px",
        },
      },
    }, 5],
  ],
  connections: [
    {
      source: "1",
      target: "4.5",
    },
    {
      source: "1",
      target: "4",
    },
    {
      source: "1",
      target: "4.5",
    },
    {
      source: "1",
      target: "5",
    },
    {
      source: "5",
      target: "2",
    },
    {
      source: "1",
      target: "3.5",
    },
  ],
};
this.setState({
  'content':
  // <DAG settings={defaultSettings} data={data}/>
    <ReactDAG
      nodes={data.nodes}
      connections={data.connections}
      // jsPlumbSettings={defaultSettings}
      // connectionDecoders={[
      //   transformConnectionDecoder,
      //   conditionConnectionDecoder,
      // ]}
      // connectionEncoders={[
      //   transformConnectionEncoder,
      //   conditionConnectionEncoder,
      // ]}
      // onChange={({
      //              nodes: n,
      //              connections: c,
      //            }: {
      //   nodes: INode[];
      //   connections: IConnectionParams[];
      // }) => {
      //   this.setState({ nodes: n, connections: c });
      // }}
      // eventListeners={eventListeners}
      // registerTypes={registerTypes}
      // zoom={this.state.zoom}
    >
      {data.nodes.map((node, i) => {
        const Component = getComponent(NodeType1);
        return <Component key={i} id={node.id}/>;
      })}
    </ReactDAG>
})