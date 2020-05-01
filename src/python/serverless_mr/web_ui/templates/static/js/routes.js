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
// import Dashboard from "views/Dashboard.jsx";
import UserProfile from "./views/UserProfile.jsx";
import TableList from "./views/TableList.jsx";
import JobInfo from "./views/JobInfo";

const dashboardRoutes = [
  // {
  //   path: "/dashboard",
  //   name: "Dashboard",
  //   icon: "pe-7s-graph",
  //   component: Dashboard,
  //   layout: "/admin"
  // },
  {
    path: "/table",
    name: "Dashboard",
    icon: "pe-7s-note2",
    component: TableList,
    layout: "/dev"
  },
  {
    path: "/register-job",
    name: "Register job",
    icon: "pe-7s-upload",
    component: UserProfile,
    layout: "/dev"
  },
  {
    path: "/job-information",
    name: "Job Information",
    icon: "pe-7s-monitor",
    component: JobInfo,
    layout: "/dev"
  }
];

export default dashboardRoutes;
