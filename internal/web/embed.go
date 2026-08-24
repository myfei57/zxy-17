// Package web embeds the four TaskFlow control pages.
package web

import _ "embed"

//go:embed tasks.html
var TasksHTML string

//go:embed schedule.html
var ScheduleHTML string

//go:embed records.html
var RecordsHTML string

//go:embed audit.html
var AuditHTML string
