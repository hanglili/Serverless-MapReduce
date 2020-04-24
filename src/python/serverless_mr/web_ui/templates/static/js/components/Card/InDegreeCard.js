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
import {inDegreeColumnNames} from "../../variables/Variables";
import {Table} from "react-bootstrap";

export class InDegreeCard extends Component {
  constructor(props){
    super(props);
    this.state = {
      content: this.props.content,
      jobName: this.props.jobName
    }
  }

  construct_table(newInDegreeData) {
    return (
      <Table hover>
        <thead>
        <tr>
          {inDegreeColumnNames.map((prop, key) => {
            return <th key={key}>{prop}</th>;
          })}
        </tr>
        </thead>
        <tbody>
        {newInDegreeData.map((prop, key) => {
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
      // const response = await fetch('http://localhost:5000/in-degree')
      //   .then(res => res.json()).then(newInDegreeData => this.construct_table(newInDegreeData))

      const res = await fetch('http://localhost:5000/in-degree?'.concat('job-name=', this.state.jobName));
      const newInDegreeData = await res.json();
      const newInDegreeArray = [];
      for(var key in newInDegreeData)
        newInDegreeArray.push([key, newInDegreeData[key]]);
      console.log("The data is " + newInDegreeArray)
      return newInDegreeArray
    } catch(e) {
      console.log(e);
      return [];
    }
  }

  componentDidMount() {
    this.interval = setInterval(async () => {
      await this.loadNewData().then(newInDegreeArray =>
        this.setState({content: this.construct_table(newInDegreeArray)})
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

export default InDegreeCard;