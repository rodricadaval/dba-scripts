 db.adminCommand({aggregate: 1,
                 pipeline: [{$currentOp: {allUsers: true, idleConnections: true}},
                            {$group: {_id: {desc: "$desc", ns: "$ns", WaitState: "$WaitState"}, count: {$sum: 1}}}],
                 cursor: {}
                });
