package com.tapdata.tm.modules.param;


import lombok.Data;

import java.util.List;
import java.util.Map;

/**
 * {
 * 	"attrs": {
 * 		"ids": [
 * 			"6155699428f0ea0052de4482",
 * 			"61a49db2728c0100ad04c049",
 * 			"61a49f4e728c0100ad04c14c"
 * 		],
 * 		"listtags": [
 *                        {
 * 				"id": "61306d951be916002487b0f6",
 * 				"value": "Data API_1212"
 *            }
 * 		]
 * 	}
 * }
 */
@Data
public class AttrsParam {
    Map attrs;
}
