/*!

=========================================================
* Light Bootstrap Dashboard React - v1.3.0
=========================================================

* Product Page: https://www.creative-tim.com/product/light-bootstrap-dashboard-react
* Copyright 2019 Creative Tim (https://www.creative-tim.com)
* Licensed under MIT (https://github.com/creativetimofficial/light-bootstrap-dashboard-react/blob/master/LICENSE.md)

* Coded by Creative Tim

=========================================================

* The above copyright notice and this permission notice shall be included in all copies or substantial portions of the Software.

*/
import React, { Component } from "react";
import { stageStateColumnNames } from "../../variables/Variables";
import {Table} from "react-bootstrap";

export class StageStateCard extends Component {
  constructor(props){
    super(props);
    this.state = {
      content: this.props.content,
      jobName: this.props.jobName
    }
  }

  construct_table(newStageStateData) {
    return (
      <Table hover>
        <thead>
        <tr>
          {stageStateColumnNames.map((prop, key) => {
            return <th key={key}>{prop}</th>;
          })}
        </tr>
        </thead>
        <tbody>
        {newStageStateData.map((prop, key) => {
          return (
            <tr key={key}>
              {prop.map((prop, key) => {
                return <td key={key}>{prop}</td>;
              })}
            </tr>
          );
        })}
        </tbody>
      </Table>
    )
  }

  async loadNewData() {
    try {
      const res = await fetch('http://localhost:5000/num-completed-operators?'.concat('job-name=', this.state.jobName));
      const newStageStateData = await res.json();
      const newStageStateArray = [];
      for(var key in newStageStateData)
        newStageStateArray.push([key, newStageStateData[key][0], newStageStateData[key][1]]);
      console.log("The data is " + newStageStateArray)
      return newStageStateArray
    } catch(e) {
      console.log(e);
      return [];
    }
  }

  componentDidMount() {
    this.interval = setInterval(async () => {
        await this.loadNewData().then(newStageStateArray =>
          this.setState({content: this.construct_table(newStageStateArray)})
        );
      }, 1000);
  }

  componentWillUnmount() {
    clearInterval(this.interval);
  }

  shouldComponentUpdate(newProps, newState, nextContext) {
    return this.state.content !== newState.content;
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

export default StageStateCard;