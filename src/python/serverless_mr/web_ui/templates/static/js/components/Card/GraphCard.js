import CytoscapeComponent from 'react-cytoscapejs';
import Cytoscape from 'cytoscape';
import dagre from 'cytoscape-dagre';
import React, {Component} from "react";

Cytoscape.use(dagre);
// Cytoscape.use(COSEBilkent);

export class GraphCard extends Component {
  constructor(props){
    super(props);
    this.state = {
      content: this.props.content,
      jobName: this.props.jobName
    }
  }

  async loadNewData() {
    try {
      const res = await fetch('http://localhost:5000/dag?'.concat('job-name=', this.state.jobName));
      return await res.json()
    } catch(e) {
      console.log(e);
      return [];
    }
  }

  componentWillMount() {
    // const layout = { name: 'random' };
    // const layout = { name: 'cose-bilkent' };
    // const layout = { name: 'grid', rows: 3 };
    const layout = {name: 'dagre', rankDir: 'LR', fit: true, spacingFactor: 1.75}
    this.loadNewData().then(dag_data => {
      const elements = []
      dag_data['nodes'].forEach(function (item, index) {
        if ('pipeline' in item) {
          elements.push({ data: { id: item['id'], parent: item['pipeline'],
              label: 'Stage' + item['id'] + '-' + item['type'] }
          })
        } else {
          elements.push({ data: { id: item['id'], label: 'Pipeline' + item['id'] }})
        }
      });
      dag_data['edges'].forEach(function (item, index) {
        elements.push({ data: item })
      });
      this.setState({
        'content': <CytoscapeComponent elements={elements} stylesheet={[
          {
            selector: 'node',
            style: {
              'label': 'data(label)',
              'text-valign': 'top',
              'text-halign': 'center',
              // 'padding': 20
            }
          },
          {
            selector: ':selected',
            css: {
              'background-color': 'black',
              'line-color': 'black',
              'target-arrow-color': 'black',
              'source-arrow-color': 'black'
            }
          },
          {
            selector: 'edge',
            style: {
              width: 1,
              'curve-style': 'bezier',
              'target-arrow-shape': 'triangle',
              'label': 'data(label)',
              'font-size': 13
            }
          }]} style={{width: '900px', height: '400px'}} layout={layout}
        />
      })
    })
  }

  render() {
    return (
      <div className={"card" + (this.props.plain ? " card-plain" : "")}>
        <div className={"header" + (this.props.hCenter ? " text-center" : "")}>
          <h4 className="title">{this.props.title}</h4>
          <p className="category">{this.props.category}</p>
        </div>
        <div
          className={
            "content" +
            (this.props.ctAllIcons ? " all-icons" : "") +
            (this.props.ctTableFullWidth ? " table-full-width" : "") +
            (this.props.ctTableResponsive ? " table-responsive" : "") +
            (this.props.ctTableUpgrade ? " table-upgrade" : "")
          }
        >
          {this.state.content}

          <div className="footer">
            {this.props.legend}
            {this.props.stats != null ? <hr /> : ""}
            <div className="stats">
              <i className={this.props.statsIcon} /> {this.props.stats}
            </div>
          </div>
        </div>
      </div>
    );
  }
}

export default GraphCard;