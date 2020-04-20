const e = React.createElement;


class Hello extends React.Component {
  render() {
    return <h1>Hello World!</h1>
  }
}

ReactDOM.render(e(Hello), document.getElementById('div_1'));
