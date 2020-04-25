const webpack = require('webpack');
const resolve = require('path').resolve;

const config = {
    devtool: 'eval-source-map',
    entry: __dirname + '/js/index.jsx',
    output:{
        path: resolve('../public/'),
        filename: 'bundle.js',
        publicPath: 'dev/public/'
        // publicPath: resolve('../public')
},
    resolve: {
        extensions: ['.js','.jsx','.css','.png','.woff','.ttf','.ico']
    },
    module: {
      rules: [
          {
              test: /\.jsx?/,
              loader: 'babel-loader',
              exclude: /node_modules/,
              query: {
                  presets: ['@babel/preset-react', '@babel/preset-env'],
                  "plugins": [
                      "@babel/plugin-proposal-class-properties",
                  ]
              }
          },
          {
              test: /\.css$/,
              loader: 'style-loader!css-loader'
          },
          {
              test: /\.s[ac]ss$/i,
              loader: 'style-loader!css-loader!sass-loader'
              // use: [
              //     // Creates `style` nodes from JS strings
              //     'style-loader',
              //     // Translates CSS into CommonJS
              //     'css-loader',
              //     // Compiles Sass to CSS
              //     'sass-loader',
              // ],
          },
          {              // exclude: [/\.js$/, /\.html$/, /\.json$/, /\.ejs$/,/\.jsx$/],
              // test: /\.(png|jpe?g|gif)$/i,
              // use: [
              //     {
              //         loader: 'file-loader',
              //     },
              // ],
              test: /\.(png|jpe?g|gif|svg)(\?v=\d+\.\d+\.\d+)?$/,
              use: [
                  {
                      loader: 'file-loader',
                      // options: {
                      //     name: '[name].[ext]',
                      //     outputPath: '/img/'
                      // }
                  }
                  ]
          },
          {
              // exclude: [/\.js$/, /\.html$/, /\.json$/, /\.ejs$/,/\.jsx$/],
              // test: /\.(png|jpe?g|gif)$/i,
              // use: [
              //     {
              //         loader: 'file-loader',
              //     },
              // ],
              test: /\.(woff(2)?|ttf|eot)(\?v=\d+\.\d+\.\d+)?$/,
              use: [
                  {
                      loader: 'file-loader',
                      // options: {
                      //     name: '[name].[ext]',
                      //     outputPath: '/fonts/'
                      // }
                  }
                  ]
          },
          ]
    }
};

module.exports = config;