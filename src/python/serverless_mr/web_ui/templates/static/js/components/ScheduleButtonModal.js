// import React, {Component} from "react";
// import {Button, Modal} from "react-bootstrap";
//
// export class ScheduleButtonModal extends Component {
//   constructor(props){
//     super(props);
//     this.state = {
//       show: false
//     }
//   }
//
//   async scheduleJob(jobName, driverLambdaName) {
//     try {
//       await fetch('http://localhost:5000/schedule-job?'.concat('job-name=', jobName,
//         '&driver-lambda-name=', driverLambdaName));
//     } catch(e) {
//       console.log(e);
//     }
//   }
//
//   // const [show, setShow] = useState(false);
//
//   handleClose = () => {
//     this.setState({
//       'show': false
//     })
//   }
//
//   handleShow = () => {
//     this.setState({
//       'show': true
//     })
//   }
//
//   render() {
//     return (
//       <>
//         <Button variant="btn btn-primary" onClick={this.handleShow()}>
//           Schedule
//         </Button>
//
//         <Modal show={false} onHide={this.handleClose()}>
//           <Modal.Header closeButton>
//             <Modal.Title>Modal heading</Modal.Title>
//           </Modal.Header>
//           <Modal.Body>Woohoo, you're reading this text in a modal!</Modal.Body>
//           <Modal.Footer>
//             <Button variant="secondary" onClick={this.handleClose()}>
//               Close
//             </Button>
//             <Button variant="primary" onClick={this.handleClose()}>
//               Save Changes
//             </Button>
//           </Modal.Footer>
//         </Modal>
//       </>
//     );
//   }
// }