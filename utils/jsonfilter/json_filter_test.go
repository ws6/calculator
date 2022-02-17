package jsonfilter

import (
	"testing"
)

var t1 = `{
  "ActingUserId": "27392365",
  "ActingUserName": "ILS_SD_NovaSeq_Integration",
  "DateCreated": "2021-11-12T21:28:16.8890000+00:00",
  "EventType": "Update",
  "FieldChanges": {
    "runstate": {
      "NewValue": "Completed",
      "OldValue": "Running"
    }
  },
  "Id": "9add51db-fbab-40c7-973e-2fa7fed4f9e4_Run_219700482",
  "IpAddress": "149.6.92.27",
  "LoggedInUserId": "28034007",
  "LoggedInUserName": "Bradley Durham",
  "Metadata": {
    "experimentname": "HCCGKDSXX_TechnicalReportTestRun"
  },
  "ResourceId": "219700482",
  "ResourceType": "Run"
}`

var t2 = `{
  "ActingUserId": "27392365",
  "ActingUserName": "ILS_SD_NovaSeq_Integration",
  "DateCreated": "2021-11-12T21:28:16.8890000+00:00",
  "EventType": "Created",
  "FieldChanges": {
    "runstate": {
      "NewValue": "Completed",
      "OldValue": "Running"
    },
     "runstate2": {
      "NewValue": "NotCompleted",
      "OldValue": "Running"
    }
  },
  "Id": "9add51db-fbab-40c7-973e-2fa7fed4f9e4_Run_219700482",
  "IpAddress": "149.6.92.27",
  "LoggedInUserId": "28034007",
  "LoggedInUserName": "Bradley Durham",
  "Metadata": {
    "experimentname": "HCCGKDSXX_TechnicalReportTestRun"
  },
  "ResourceId": "219700482",
  "ResourceType": "Run"
}`

var t3 = `{  "ActingUserId": "27392365",  "ActingUserName": "ILS_SD_NovaSeq_Integration",  "ApplicationId": "1375375",  "DateCreated": "2022-01-13T23:40:46.6410000+00:00",  "EventType": "Create",  "FieldChanges": {    "application": {      "NewValue": "12760748"    },    "deliverystatus": {      "NewValue": "None"    },    "name": {      "NewValue": "LP9000124-DNA_A05-ILS_DRAGEN_GL_2.0.2 2022-01-13 23:40:45Z"    },    "qcstatus": {      "NewValue": "Undefined"    },    "status": {      "NewValue": "Pending"    },    "user": {      "NewValue": "27392365"    }  },  "Id": "df0bc931-eab5-4362-b3ad-fd032e1b736b_AppSession_512656144",  "IpAddress": "192.84.34.98",  "LoggedInUserId": "29052023",  "LoggedInUserName": "Jingtao Liu",  "Metadata": {    "applicationname": "ILS_DRAGEN_GL_2.0.2",    "usercreatedbyname": "ILS_SD_NovaSeq_Integration"  },  "ResourceId": "512656144",  "ResourceType": "AppSession"}`

func TestEvalString(t *testing.T) {

	cfg0 := map[string]string{
		KEYS:                             `EventType;FieldChanges.runstate.NewValue`,
		"EventType":                      "Create",
		"FieldChanges.runstate.NewValue": "Completed",
		// "FieldChanges.runstate2.NewValue": "NotCompleted",
	}
	if !EvaluateString(cfg0, []byte(t3)) {
		t.Fatal(`should  pass`)
	}

	return

	cfg := map[string]string{
		KEYS:                             `EventType;FieldChanges.runstate.NewValue`,
		"EventType":                      "Update",
		"FieldChanges.runstate.NewValue": "Completed;NotCompleted",
	}
	if !EvaluateString(cfg, []byte(t1)) {
		t.Fatal(`failed`)
	}

	cfg2 := map[string]string{
		KEYS:                             `EventType;FieldChanges.runstate.NewValue`,
		"EventType":                      "*",
		"FieldChanges.runstate.NewValue": "Completed",
	}
	if !EvaluateString(cfg2, []byte(t1)) {
		t.Fatal(`failed`)
	}
	cfg3 := map[string]string{
		KEYS:                             `EventType;FieldChanges.runstate.NewValue`,
		"EventType":                      "Created",
		"FieldChanges.runstate.NewValue": "Completed",
	}
	if EvaluateString(cfg3, []byte(t1)) {
		t.Fatal(`should not pass`)
	}

	t.Log(`t4`)
	cfg4 := map[string]string{
		KEYS:                              `EventType;FieldChanges.runstate.NewValue;FieldChanges.runstate2.NewValue`,
		"EventType":                       "Created",
		"FieldChanges.runstate.NewValue":  "Completed",
		"FieldChanges.runstate2.NewValue": "NotCompleted",
	}
	if !EvaluateString(cfg4, []byte(t2)) {
		t.Fatal(`should  pass`)
	}

}
