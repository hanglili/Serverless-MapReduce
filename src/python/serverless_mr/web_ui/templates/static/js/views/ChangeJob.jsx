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
import "regenerator-runtime/runtime.js";
import {
  Grid,
  Row,
  Col,
  FormControl,
  ControlLabel,
  FormGroup
} from "react-bootstrap";

import { Card } from "../components/Card/Card.jsx";
import Button from "../components/CustomButton/CustomButton.jsx";

class ChangeJob extends Component {
  constructor(props){
    super(props);
    console.log(this.props.jobName);
    this.state = {
      jobName: this.props.location.state.jobName,
      sourceFiles: {}
    }
  }

  async loadSourceCode() {
    try {
      const currentPageHost = location.host;
      const currentPageHostname = location.hostname;
      const currentPageProtocol = location.protocol;
      let url = '';
      if (currentPageHostname === "localhost" || currentPageHostname === "127.0.0.1") {
        url = `${currentPageProtocol}//${currentPageHost}/get-source-files?`;
      } else {
        url = `${currentPageProtocol}//${currentPageHostname}/dev/get-source-files?`;
      }
      url = url.concat('job-name=', this.state.jobName);
      const res = await fetch(url);
      return await res.json()
    } catch(e) {
      console.log(e);
      return [];
    }
  }

  componentWillMount() {
    this.loadSourceCode().then(source_files => {
      this.setState({
        'sourceFiles': source_files
      });
    });
  }

  async registerJob(sourceFiles) {
    try {
      let data = new URLSearchParams();
      const currentPageHost = location.host;
      const currentPageHostname = location.hostname;
      const currentPageProtocol = location.protocol;
      let url = '';
      if (currentPageHostname === "localhost" || currentPageHostname === "127.0.0.1") {
        url = `${currentPageProtocol}//${currentPageHost}/modify-job?`;
      } else {
        url = `${currentPageProtocol}//${currentPageHostname}/dev/modify-job?`;
      }
      // let formBody = new FormData();
      // formBody.set("driver.json", driver);
      // formBody.set("static-job-info.json", staticJobInfo);
      // formBody.set("user_main.py", userMain);
      // formBody.set("functions.py", functions);
      // let formBody = [];
      // formBody.push(encodeURIComponent("driver.json") + "=" + encodeURIComponent(driver));
      // formBody.push(encodeURIComponent("static-job-info.json") + "=" + encodeURIComponent(staticJobInfo));
      // formBody.push(encodeURIComponent("user_main.py") + "=" + encodeURIComponent(userMain));
      // formBody.push(encodeURIComponent("functions.py") + "=" + encodeURIComponent(functions));
      // console.log(formBody);
      for (const filename in sourceFiles){
        console.log(filename);
        console.log(sourceFiles[filename]);
        data.append(filename, sourceFiles[filename]);
      }

      await fetch(url, {
        method: 'POST',
        // body: formBody.join("&"),
        body: data,
        headers: {
          'Content-Type': 'application/x-www-form-urlencoded; charset=UTF-8'
        }
      });
    } catch(e) {
      console.log(e);
    }
  }

  render() {
    return (
      <div className="content">
        <Grid fluid>
          <Row>
            <Col md={8}>
              <Card
                title="Register Job"
                content={
                  <form>
                    {Object.keys(this.state.sourceFiles).map((prop, key) => {
                      return <Row>
                              <Col md={12}>
                                <FormGroup controlId="formControlsTextarea">
                                  <ControlLabel>{prop}</ControlLabel>
                                  <FormControl
                                    rows="15"
                                    componentClass="textarea"
                                    bsClass="form-control"
                                    inputRef={node => this[`${prop}`] = node}
                                    // placeholder=""
                                    // inputRef={userMain => this.setState({ 'userMain': userMain } )}
                                    defaultValue={this.state.sourceFiles[prop]}
                                  />
                                </FormGroup>
                              </Col>
                            </Row>;
                    })}
                    <Button bsStyle="info" pullRight fill onClick={() => {
                      const inputValues = {};
                      for (const filename in this.state.sourceFiles){
                        inputValues[filename] = this[`${filename}`].value;
                      }
                      this.registerJob(inputValues);
                    }}>
                      Register Job
                    </Button>
                    <div className="clearfix" />
                  </form>
                }
              />
            </Col>
          </Row>
        </Grid>
      </div>
    );
  }
}

export default ChangeJob;
