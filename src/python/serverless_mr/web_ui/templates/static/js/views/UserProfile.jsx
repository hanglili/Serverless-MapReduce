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
  constructor(props){
    super(props);
    this.state = {
      driver: "{}",
      staticJobInfo: "{}",
      userMain: "{}",
      functions: "{}"
    }
  }

   async registerJob(driver, staticJobInfo, userMain, functions) {
    try {
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
      let formBody = [];
      formBody.push("driver.json" + "=" + driver);
      formBody.push("static-job-info.json" + "=" + staticJobInfo);
      formBody.push("user_main.py" + "=" + userMain);
      formBody.push("functions.py" + "=" + functions);
      await fetch(url, {
        method: 'POST',
        body: formBody.join("&"),
        // body: formBody,
        headers: {
          'Content-Type': 'application/x-www-form-urlencoded'
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
                          <ControlLabel>driver.json</ControlLabel>
                          <FormControl
                            rows="15"
                            componentClass="textarea"
                            bsClass="form-control"
                            placeholder="{}"
                            // inputRef={driver => this.setState({ 'driver': driver })}
                            defaultValue="{}"
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
                            placeholder="{}"
                            // inputRef={staticJobInfo => this.setState({ 'staticJobInfo': staticJobInfo } )}
                            defaultValue="{}"
                          />
                        </FormGroup>
                      </Col>
                    </Row>
                    <Row>
                      <Col md={12}>
                        <FormGroup controlId="formControlsTextarea">
                          <ControlLabel>user_main.py</ControlLabel>
                          <FormControl
                            rows="15"
                            componentClass="textarea"
                            bsClass="form-control"
                            placeholder=""
                            // inputRef={userMain => this.setState({ 'userMain': userMain } )}
                            defaultValue=""
                          />
                        </FormGroup>
                      </Col>
                    </Row>
                    <Row>
                      <Col md={12}>
                        <FormGroup controlId="formControlsTextarea">
                          <ControlLabel>functions.py</ControlLabel>
                          <FormControl
                            rows="15"
                            componentClass="textarea"
                            bsClass="form-control"
                            placeholder=""
                            // inputRef={functions => this.setState({ 'functions': functions } )}
                            defaultValue=""
                          />
                        </FormGroup>
                      </Col>
                    </Row>
                    <Button bsStyle="info" pullRight fill onClick={() => {
                      this.registerJob(this.state.driver, this.state.staticJobInfo,
                          this.state.userMain, this.state.functions);
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
