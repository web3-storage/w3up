import { nodeResolve } from '@rollup/plugin-node-resolve';
import commonjs from '@rollup/plugin-commonjs';

export default {
  input: 'src/main.js',
  output: {
    dir: 'dist',
    format: 'es'
  },
  plugins: [nodeResolve({ browser: true }), commonjs()]
};