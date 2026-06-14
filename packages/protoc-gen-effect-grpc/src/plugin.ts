#!/usr/bin/env node
import { runNodeJs } from "@bufbuild/protoplugin";

import { plugin } from "./pluginDefinition.js";

export { plugin } from "./pluginDefinition.js";

runNodeJs(plugin);
