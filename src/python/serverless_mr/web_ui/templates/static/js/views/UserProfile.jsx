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

class UserProfile extends Component {
   async registerJob(driver, staticJobInfo, userMain, functions) {
    try {
      let data = new URLSearchParams();
      console.log("Register job parameters");
      console.log(driver);
      console.log(staticJobInfo);
      console.log(userMain);
      console.log(functions);
      const currentPageHost = location.host;
      const currentPageHostname = location.hostname;
      const currentPageProtocol = location.protocol;
      let url = '';
      if (currentPageHostname === "localhost" || currentPageHostname === "127.0.0.1") {
        url = `${currentPageProtocol}//${currentPageHost}/register-job?`;
      } else {
        url = `${currentPageProtocol}//${currentPageHostname}/dev/register-job?`;
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
      data.append('driver.json', driver);
      data.append('static-job-info.json', staticJobInfo);
      data.append('user_main.py', userMain);
      data.append('functions.py', functions);
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
                    <Row>
                      <Col md={12}>
                        <FormGroup controlId="formControlsTextarea">
                          <ControlLabel>user_main.py</ControlLabel>
                          <FormControl
                            rows="15"
                            componentClass="textarea"
                            bsClass="form-control"
                            inputRef={node => this.userMainNode = node}
                            // placeholder=""
                            // inputRef={userMain => this.setState({ 'userMain': userMain } )}
                            defaultValue="from serverless_mr.main import ServerlessMR&#13;&#10;from user_functions.functions import *"
                          />
                        </FormGroup>
                      </Col>
                    </Row>
                    <Row>
                      <Col md={12}>
                        <FormGroup controlId="formControlsTextarea">
                          <ControlLabel>user_functions/functions.py</ControlLabel>
                          <FormControl
                            rows="15"
                            componentClass="textarea"
                            bsClass="form-control"
                            inputRef={node => this.functionsNode = node}
                            // placeholder=""
                            // inputRef={functions => this.setState({ 'functions': functions } )}
                            defaultValue=""
                          />
                        </FormGroup>
                      </Col>
                    </Row>
                    <Row>
                      <Col md={12}>
                        <FormGroup controlId="formControlsTextarea">
                          <ControlLabel>static-job-info.json</ControlLabel>
                          <FormControl
                            rows="15"
                            componentClass="textarea"
                            bsClass="form-control"
                            inputRef={node => this.staticJobInfoNode = node}
                            placeholder="{}"
                            // inputRef={staticJobInfo => this.setState({ 'staticJobInfo': staticJobInfo } )}
                            defaultValue="{&#13;&#10;
                                  &quot;jobName&quot;: &quot;bl-release-1&quot;,&#13;&#10;
                                  &quot;shufflingBucket&quot;: &quot;serverless-mapreduce-storage&quot;,&#13;&#10;
                                  &quot;inputSourceType&quot;: &quot;s3&quot;,&#13;&#10;
                                  &quot;inputSource&quot;: &quot;serverless-mapreduce-storage-input&quot;,&#13;&#10;
                                  &quot;outputSourceType&quot;: &quot;s3&quot;,&#13;&#10;
                                  &quot;outputSource&quot;: &quot;serverless-mapreduce-storage-output&quot;&#13;&#10;}"
                          />
                        </FormGroup>
                      </Col>
                    </Row>
                    <Row>
                      <Col md={12}>
                        <FormGroup controlId="formControlsTextarea">
                          <ControlLabel>driver.json</ControlLabel>
                          <FormControl
                            rows="15"
                            componentClass="textarea"
                            bsClass="form-control"
                            placeholder="{}"
                            inputRef={node => this.driverNode = node}
                            defaultValue="{}"
                          />
                        </FormGroup>
                      </Col>
                    </Row>
                    <Button bsStyle="info" pullRight fill onClick={() => {
                      this.registerJob(this.driverNode.value, this.staticJobInfoNode.value,
                          this.userMainNode.value, this.functionsNode.value);
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

export default UserProfile;
