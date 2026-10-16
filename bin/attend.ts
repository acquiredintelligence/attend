#!/usr/bin/env -S node --experimental-strip-types
import { cli } from '../src/server/serve.ts';

process.exitCode = await cli(process.argv.slice(2), process.env);
