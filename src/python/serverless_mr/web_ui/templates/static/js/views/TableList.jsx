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
import React, {Component} from "react";
import {Grid, Row, Col, Table, Button, Modal} from "react-bootstrap";
import { withRouter } from "react-router-dom";
import "regenerator-runtime/runtime.js";

import Card from "../components/Card/Card.jsx";
import { jobsColumnNames, registeredJobsColumnNames } from "../variables/Variables.jsx";
import {StatsCard} from "../components/StatsCard/StatsCard";


class TableList extends Component {
  constructor(props){
    super(props);
    this.state = {
      completed: [],
      active: [],
      registered: [],
      numActiveJobs: 0,
      numCompletedJobs: 0,
      username: "",
      show: false
    }
  }

  handleClose(){
    this.setState({
      'show': false
    })
  }

  handleShow(){
    this.setState({
      'show': true
    })
  }

  async loadUsername() {
    try {
      const currentPageHost = location.host;
      const currentPageHostname = location.hostname;
      const currentPageProtocol = location.protocol;
      let url = '';
      if (currentPageHostname === "localhost" || currentPageHostname === "127.0.0.1") {
        url = `${currentPageProtocol}//${currentPageHost}/username`;
      } else {
        url = `${currentPageProtocol}//${currentPageHostname}/dev/username`;
      }
      const res = await fetch(url);
      return await res.text();
    } catch(e) {
      console.log(e);
      return "";
    }
  }

  async loadNewData() {
    try {
      const currentPageHost = location.host;
      const currentPageHostname = location.hostname;
      const currentPageProtocol = location.protocol;
      let url = '';
      if (currentPageHostname === "localhost" || currentPageHostname === "127.0.0.1") {
        url = `${currentPageProtocol}//${currentPageHost}/jobs`;
      } else {
        url = `${currentPageProtocol}//${currentPageHostname}/dev/jobs`;
      }
      const res = await fetch(url);
      return await res.json();
    } catch(e) {
      console.log(e);
      return [];
    }
  }

  reformatJobsData(jobsData, fieldNames) {
    const jobArray = [];
    for(var k = 0; k < jobsData.length; k++) {
      const map = jobsData[k];
      const currentRow = [];
      for(const fieldName of fieldNames) {
        currentRow.push(map[fieldName])
      }
      jobArray.push(currentRow)
    }
    return jobArray
  }

  updateJobsData(newJobsData) {
    if (newJobsData.length === 0) {
      return;
    }
    const newRegisteredJobArray = this.reformatJobsData(newJobsData.registered,
      [
        'jobName', 'driverLambdaName', 'shufflingBucket', 'inputSource',
        'outputSource', 'registeredTime', 'totalNumPipelines', 'totalNumStages'
      ]);
    const newCompletedJobArray = this.reformatJobsData(newJobsData.completed,
      [
        'jobName', 'shufflingBucket', 'inputSource', 'outputSource',
        'submissionTime', 'duration', 'totalNumPipelines', 'totalNumStages'
      ]);
    const newActiveJobArray = this.reformatJobsData(newJobsData.active,
      [
        'jobName', 'shufflingBucket', 'inputSource', 'outputSource',
        'submissionTime', 'duration', 'totalNumPipelines', 'totalNumStages'
      ]);
    this.setState({
      'registered': newRegisteredJobArray,
      'completed': newCompletedJobArray,
      'active': newActiveJobArray,
      'numActiveJobs': newActiveJobArray.length,
      'numCompletedJobs': newCompletedJobArray.length
    })
  }

  componentWillMount() {
    this.loadUsername().then(username => {
      this.setState({
        'username': username
      })
    });
    this.loadNewData().then(newJobsData => {
      this.updateJobsData(newJobsData)
    });
    this.interval = setInterval(async () => {
      await this.loadNewData().then(newJobsData => {
        this.updateJobsData(newJobsData)
      })
    }, 5000);
  }

  componentWillUnmount() {
    clearInterval(this.interval);
  }

  onClickHandler = (e, prop) => {
    this.props.history.push({
      pathname: "job-information",
      state: { jobName: prop[0] }
    })
  };

  handleScheduleExpressionChange = (e) => {
    this.setState({
      'scheduleExpression': e.target.value
    });
  };

  async invokeJob(jobName, driverLambdaName) {
    try {
      const currentPageHost = location.host;
      const currentPageHostname = location.hostname;
      const currentPageProtocol = location.protocol;
      let url = '';
      if (currentPageHostname === "localhost" || currentPageHostname === "127.0.0.1") {
        url = `${currentPageProtocol}//${currentPageHost}/invoke-job?`;
      } else {
        url = `${currentPageProtocol}//${currentPageHostname}/dev/invoke-job?`;
      }
      url = url.concat('job-name=', jobName, '&driver-lambda-name=', driverLambdaName);
      await fetch(url);
    } catch(e) {
      console.log(e);
    }
  }

  async invokeSchedule(jobName, driverLambdaName, scheduleExpression) {
    try {
      const currentPageHost = location.host;
      const currentPageHostname = location.hostname;
      const currentPageProtocol = location.protocol;
      let url = '';
      if (currentPageHostname === "localhost" || currentPageHostname === "127.0.0.1") {
        url = `${currentPageProtocol}//${currentPageHost}/schedule-job?`;
      } else {
        url = `${currentPageProtocol}//${currentPageHostname}/dev/schedule-job?`;
      }
      url = url.concat('job-name=', jobName, '&driver-lambda-name=', driverLambdaName,
          '&schedule-expression=', scheduleExpression);
      await fetch(url);
      // await fetch('http://localhost:5000/schedule-job?'.concat('job-name=', jobName,
      //   '&driver-lambda-name=', driverLambdaName, '&schedule-expression=', scheduleExpression));
    } catch(e) {
      console.log(e);
    }
  }

  render() {
    return (
      <div className="content">
        <Grid fluid>
          <Row>
            <Col lg={5} sm={6}>
              <StatsCard
                bigIcon={<i className="pe-7s-user text-warning" />}
                statsText="AWS Account ID"
                statsValue={this.state.username}
                statsIcon={<i className="fa fa-refresh" />}
                statsIconText="Updated now"
              />
            </Col>
            <Col lg={3} sm={6}>
              <StatsCard
                bigIcon={<i className="pe-7s-folder text-success" />}
                statsText="Active jobs"
                statsValue={this.state.numActiveJobs}
                statsIcon={<i className="fa fa-refresh" />}
                statsIconText="Updated now"
              />
            </Col>
            <Col lg={4} sm={6}>
              <StatsCard
                bigIcon={<i className="pe-7s-folder text-success" />}
                statsText="Completed jobs"
                statsValue={this.state.numCompletedJobs}
                statsIcon={<i className="fa fa-refresh" />}
                statsIconText="Updated now"
              />
            </Col>
          </Row>
          <Row>
            <Col md={12}>
              <Card
                title="Registered Jobs"
                category="Current registered jobs"
                ctTableFullWidth
                ctTableResponsive
                content={
                  <Table hover>
                    <thead>
                    <tr>
                      {registeredJobsColumnNames.map((prop, key) => {
                        return <th key={key} style={{'textAlign': 'center', 'verticalAlign': 'middle'}}>{prop}</th>;
                      })}
                    </tr>
                    </thead>
                    <tbody>
                    {this.state.registered.map((prop, key) => {
                      return (
                        <tr>
                          {prop.map((prop, key) => {
                            return <td key={key} style={{'textAlign': 'center', 'verticalAlign': 'middle'}}>
                              {prop}
                            </td>;
                          })}
                          <td key={8}>
                            <Button bsClass="btn btn-primary" onClick={async () => {
                              await this.invokeJob(prop[0], prop[1])
                            }}>
                              Run
                            </Button>
                          </td>
                          <td key={9}>
                            <>
                              <Button bsClass="btn btn-primary" onClick={this.handleShow.bind(this)}>
                                Schedule
                              </Button>
                              <Modal show={this.state.show} onHide={this.handleClose.bind(this)}>
                                <Modal.Header closeButton>
                                  <Modal.Title>Scheduling Job</Modal.Title>
                                </Modal.Header>
                                <Modal.Body>
                                  <form>
                                    <input style={{'borderRadius': '5px', 'padding': '20px',
                                      'width': '200px', 'height': '15px'}}
                                           type="text" name="Schedule expression" placeholder="Schedule expression"
                                           value={this.state.scheduleExpression} onChange={this.handleScheduleExpressionChange} />
                                  </form>
                                </Modal.Body>
                                <Modal.Footer>
                                  <Button bsClass="btn btn-primary" onClick={this.handleClose.bind(this)}>
                                    Close
                                  </Button>
                                  <Button bsClass="btn btn-primary" onClick={() => {
                                    // this.invokeSchedule.bind(this, prop[0], prop[1]);
                                    this.invokeSchedule(prop[0], prop[1], this.state.scheduleExpression);
                                    this.handleClose();
                                    this.setState({'scheduleExpression': ''})
                                  }}>
                                    Schedule
                                  </Button>
                                </Modal.Footer>
                              </Modal>
                            </>
                          </td>
                        </tr>
                      );
                    })}
                    </tbody>
                  </Table>
                }
              />
            </Col>

            <Col md={12}>
              <Card
                title="Active Jobs"
                category="Current active jobs"
                ctTableFullWidth
                ctTableResponsive
                content={
                  <Table hover>
                    <thead>
                      <tr>
                        {jobsColumnNames.map((prop, key) => {
                          return <th key={key} style={{'textAlign': 'center', 'verticalAlign': 'middle'}}>{prop}</th>;
                        })}
                      </tr>
                    </thead>
                    <tbody>
                      {this.state.active.map((prop, key) => {
                        return (
                          <tr key={key} onClick={(e) => this.onClickHandler(e, prop)}>
                            {prop.map((prop, key) => {
                              return <td key={key} style={{'textAlign': 'center', 'verticalAlign': 'middle'}}>
                                {prop}
                              </td>;
                            })}
                          </tr>
                        );
                      })}
                    </tbody>
                  </Table>
                }
              />
            </Col>

            <Col md={12}>
              <Card
                title="Completed Jobs"
                category="Current completed jobs"
                ctTableFullWidth
                ctTableResponsive
                content={
                  <Table hover>
                    <thead>
                      <tr>
                        {jobsColumnNames.map((prop, key) => {
                          return <th key={key} style={{'textAlign': 'center', 'verticalAlign': 'middle'}}>{prop}</th>;
                        })}
                      </tr>
                    </thead>
                    <tbody>
                      {this.state.completed.map((prop, key) => {
                        return (
                          <tr key={key} onClick={(e) => this.onClickHandler(e, prop)}>
                            {prop.map((prop, key) => {
                              return <td key={key} style={{'textAlign': 'center', 'verticalAlign': 'middle'}}>
                                {prop}
                              </td>;
                            })}
                          </tr>
                        );
                      })}
                    </tbody>
                  </Table>
                }
              />
            </Col>
          </Row>
        </Grid>
      </div>
    );
  }
}

// export default TableList;
export default withRouter(TableList);
