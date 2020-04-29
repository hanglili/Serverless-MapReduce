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
import { stageProgressColumnNames } from "../../variables/Variables";
import { Table, ProgressBar } from "react-bootstrap";

export class StageProgressCard extends Component {
  constructor(props){
    super(props);
    this.state = {
      content: this.props.content,
      jobName: this.props.jobName,
      submissionTime: this.props.submissionTime
    }
  }

  construct_table(newStageProgressData) {
    return (
      <Table hover>
        <thead>
        <tr>
          {stageProgressColumnNames.map((prop, key) => {
            return <th key={key}>{prop}</th>;
          })}
        </tr>
        </thead>
        <tbody>
        {newStageProgressData.map((prop, key) => {
          return (
            <tr key={key}>
              <td key={0}>{prop[0]}</td>
              <td key={1}>
                <div>
                  <ProgressBar animated now={prop[1] / prop[2] * 100} label={`${prop[1]} / ${prop[2]}`}/>
                </div>
              </td>
            </tr>
          );
        })}
        </tbody>
      </Table>
    )
  }

  async loadNewData() {
    try {
      const currentPageHost = location.host;
      const currentPageHostname = location.hostname;
      const currentPageProtocol = location.protocol;
      let url = '';
      if (currentPageHostname === "localhost" || currentPageHostname === "127.0.0.1") {
        url = `${currentPageProtocol}//${currentPageHost}/stage-progress?`;
      } else {
        url = `${currentPageProtocol}//${currentPageHostname}/dev/stage-progress?`;
      }
      url = url.concat('job-name=', this.state.jobName, '&submission-time=', this.state.submissionTime);
      const res = await fetch(url);
      const newStageProgressData = await res.json();
      const newStageProgressArray = [];
      for(var key in newStageProgressData)
        newStageProgressArray.push([key, parseInt(newStageProgressData[key][0], 10),
          parseInt(newStageProgressData[key][1], 10)]);
      console.log("The data is " + newStageProgressArray)
      return newStageProgressArray
    } catch(e) {
      console.log(e);
      return [];
    }
  }

  componentDidMount() {
    this.interval = setInterval(async () => {
      await this.loadNewData().then(newStageProgressArray =>
        this.setState({content: this.construct_table(newStageProgressArray)})
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

export default StageProgressCard;