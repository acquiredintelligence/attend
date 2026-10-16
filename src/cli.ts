#!/usr/bin/env node
import { cli } from './server/serve.js';

process.exitCode = await cli(process.argv.slice(2), process.env);
