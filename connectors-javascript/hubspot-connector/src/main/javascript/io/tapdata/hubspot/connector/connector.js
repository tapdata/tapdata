config.setStreamReadIntervalSeconds(60);
var batchStart = Date.parse(new Date());
var afterData;
var clientInfo = {
    "client_id": "fe877fa9-96bf-49f5-8f62-cc7f658a26d6",
    "client_secret": "8eefb54e-7b27-417a-b319-c958d331685e",
}

function discoverSchema(connectionConfig) {
    return [
        {
        "name": "company",
            "fields": {
                'id':{
                  'type':'String',
                  'comment':'',
                  'isPrimaryKey': true,
                  'primaryKeyPos': 1
                },
                "hs_analytics_first_timestamp": {
                    "type": "DateTime",
                    "comment": ""
                },
                "hs_analytics_last_timestamp": {
                    "type": "DateTime",
                    "comment": ""
                },
                "hs_analytics_last_visit_timestamp": {
                    "type": "DateTime",
                    "comment": ""
                },
                "hs_analytics_num_page_views": {
                    "type": "Number",
                    "comment": ""
                },
                "hs_analytics_num_visits": {
                    "type": "Number",
                    "comment": ""
                },
                "engagements_last_meeting_booked": {
                    "type": "DateTime",
                    "comment": ""
                },
                "engagements_last_meeting_booked_campaign": {
                    "type": "String",
                    "comment": ""
                },
                "engagements_last_meeting_booked_source": {
                    "type": "String",
                    "comment": ""
                },
                "hs_last_booked_meeting_date": {
                    "type": "DateTime",
                    "comment": ""
                },
                "hs_last_logged_call_date": {
                    "type": "DateTime",
                    "comment": ""
                },
                "hs_last_open_task_date": {
                    "type": "DateTime",
                    "comment": ""
                },
                "hs_last_sales_activity_timestamp": {
                    "type": "DateTime",
                    "comment": ""
                },
                "hs_lastmodifieddate": {
                    "type": "DateTime",
                    "comment": ""
                },
                "notes_last_contacted": {
                    "type": "DateTime",
                    "comment": ""
                },
                "notes_last_updated": {
                    "type": "DateTime",
                    "comment": ""
                },
                "notes_next_activity_date": {
                    "type": "DateTime",
                    "comment": ""
                },
                "num_contacted_notes": {
                    "type": "Number",
                    "comment": ""
                },
                "about_us": {
                    "type": "String",
                    "comment": ""
                },
                "address": {
                    "type": "String",
                    "comment": ""
                },
                "address2": {
                    "type": "String",
                    "comment": ""
                },
                "annualrevenue": {
                    "type": "Number",
                    "comment": ""
                },
                "city": {
                    "type": "String",
                    "comment": ""
                },
                "closedate": {
                    "type": "DateTime",
                    "comment": ""
                },
                "country": {
                    "type": "String",
                    "comment": ""
                },
                "createdate": {
                    "type": "DateTime",
                    "comment": ""
                },
                "days_to_close": {
                    "type": "Number",
                    "comment": ""
                },
                "description": {
                    "type": "String",
                    "comment": ""
                },
                "domain": {
                    "type": "String",
                    "comment": ""
                },
                "engagements_last_meeting_booked_medium": {
                    "type": "String",
                    "comment": ""
                },
                "first_contact_createdate": {
                    "type": "DateTime",
                    "comment": ""
                },
                "founded_year": {
                    "type": "String",
                    "comment": ""
                },
                "hs_analytics_last_touch_converting_campaign": {
                    "type": "String",
                    "comment": ""
                },
                "hs_analytics_latest_source": {
                    "type": "Enumeration",
                    "comment": ""
                },
                "hs_analytics_source_data_1": {
                    "type": "String",
                    "comment": ""
                },
                "hs_analytics_source_data_2": {
                    "type": "String",
                    "comment": ""
                },
                "hs_created_by_user_id": {
                    "type": "Number",
                    "comment": ""
                },
                "hs_createdate": {
                    "type": "DateTime",
                    "comment": ""
                },
                "hs_merged_object_ids": {
                    "type": "Enumeration",
                    "comment": ""
                },
                "hs_num_child_companies": {
                    "type": "Number",
                    "comment": ""
                },
                "hs_object_id": {
                    "type": "Number",
                    "comment": ""
                },
                "hs_parent_company_id": {
                    "type": "Number",
                    "comment": ""
                },
                "industry": {
                    "type": "Enumeration",
                    "comment": ""
                },
                "is_public": {
                    "type": "Boolean",
                    "comment": ""
                },
                "lifecyclestage": {
                    "type": "Enumeration",
                    "comment": ""
                },
                "name": {
                    "type": "String",
                    "comment": ""
                },
                "num_associated_contacts": {
                    "type": "Number",
                    "comment": ""
                },
                "numberofemployees": {
                    "type": "Number",
                    "comment": ""
                },
                "phone": {
                    "type": "String",
                    "comment": ""
                },
                "state": {
                    "type": "String",
                    "comment": ""
                },
                "timezone": {
                    "type": "String",
                    "comment": ""
                },
                "total_money_raised": {
                    "type": "String",
                    "comment": ""
                },
                "total_revenue": {
                    "type": "Number",
                    "comment": ""
                },
                "type": {
                    "type": "Enumeration",
                    "comment": ""
                },
                "web_technologies": {
                    "type": "Enumeration",
                    "comment": ""
                },
                "website": {
                    "type": "String",
                    "comment": ""
                },
                "zip": {
                    "type": "String",
                    "comment": ""
                },
                "first_conversion_date": {
                    "type": "DateTime",
                    "comment": ""
                },
                "first_conversion_event_name": {
                    "type": "String",
                    "comment": ""
                },
                "hs_analytics_first_touch_converting_campaign": {
                    "type": "String",
                    "comment": ""
                },
                "hs_analytics_first_visit_timestamp": {
                    "type": "DateTime",
                    "comment": ""
                },
                "num_conversion_events": {
                    "type": "Number",
                    "comment": ""
                },
                "recent_conversion_date": {
                    "type": "DateTime",
                    "comment": ""
                },
                "recent_conversion_event_name": {
                    "type": "String",
                    "comment": ""
                },
                "first_deal_created_date": {
                    "type": "DateTime",
                    "comment": ""
                },
                "hs_num_open_deals": {
                    "type": "Number",
                    "comment": ""
                },
                "hs_total_deal_value": {
                    "type": "Number",
                    "comment": ""
                },
                "num_associated_deals": {
                    "type": "Number",
                    "comment": ""
                },
                "recent_deal_amount": {
                    "type": "Number",
                    "comment": ""
                },
                "recent_deal_close_date": {
                    "type": "DateTime",
                    "comment": ""
                },
                "hs_lead_status": {
                    "type": "Enumeration",
                    "comment": ""
                },
                "hubspot_owner_assigneddate": {
                    "type": "DateTime",
                    "comment": ""
                },
                "hubspot_owner_id": {
                    "type": "Enumeration",
                    "comment": ""
                },
                 "hubspot_team_id": {
                    "type": "Enumeration",
                    "comment": ""
                },
                "facebook_company_page": {
                    "type": "String",
                    "comment": ""
                },
                "facebookfans": {
                    "type": "Number",
                    "comment": ""
                },
                "googleplus_page": {
                    "type": "String",
                    "comment": ""
                },
                "linkedin_company_page": {
                    "type": "String",
                    "comment": ""
                },
                "linkedinbio": {
                    "type": "String",
                    "comment": ""
                },
                "twitterbio": {
                    "type": "String",
                    "comment": ""
                },
                "twitterfollowers": {
                    "type": "Number",
                    "comment": ""
                },
                "twitterhandle": {
                    "type": "String",
                    "comment": ""
                },
                "hs_ideal_customer_profile": {
                    "type": "Enumeration",
                    "comment": ""
                },
                "hs_is_target_account": {
                    "type": "Boolean",
                    "comment": ""
                },
                "hs_num_blockers": {
                    "type": "Number",
                    "comment": ""
                },
                "hs_num_contacts_with_buying_roles": {
                    "type": "Number",
                    "comment": ""
                },
                "hs_num_decision_makers": {
                    "type": "Number",
                    "comment": ""
                }
            }
        },
        {
            "name": "contact",
            "fields": {
            'id':{
                              'type':'String',
                              'comment':'',
                               'isPrimaryKey':true,
                  'primaryKeyPos': 1
                            },
                "hs_analytics_average_page_views": {
                "type": "number",
                "comment": ""
                },
                "hs_analytics_first_referrer": {
                "type": "string",
                "comment": ""
                },
                "hs_analytics_first_timestamp": {
                "type": "datetime",
                "comment": ""
                },
                "hs_analytics_first_touch_converting_campaign": {
                "type": "string",
                "comment": ""
                },
                "hs_analytics_first_url": {
                "type": "string",
                "comment": ""
                },
                "hs_analytics_first_visit_timestamp": {
                "type": "datetime",
                "comment": ""
                },
                "hs_analytics_last_referrer": {
                "type": "string",
                "comment": ""
                },
                "hs_analytics_last_timestamp": {
                "type": "datetime",
                "comment": ""
                },
                "hs_analytics_last_touch_converting_campaign": {
                "type": "string",
                "comment": ""
                },
                "hs_analytics_last_url": {
                "type": "string",
                "comment": ""
                },
                "hs_analytics_last_visit_timestamp": {
                "type": "datetime",
                "comment": ""
                },
                "hs_analytics_num_event_completions": {
                "type": "number",
                "comment": ""
                },
                "hs_analytics_num_page_views": {
                "type": "number",
                "comment": ""
                },
                "hs_analytics_num_visits": {
                "type": "number",
                "comment": ""
                },
                "hs_analytics_revenue": {
                "type": "number",
                "comment": ""
                },
                "hs_analytics_source": {
                "type": "enumeration",
                "comment": ""
                },
                "hs_analytics_source_data_1": {
                "type": "string",
                "comment": ""
                },
                "hs_analytics_source_data_2": {
                "type": "string",
                "comment": ""
                },
                "hs_latest_source": {
                "type": "enumeration",
                "comment": ""
                },
                "hs_latest_source_data_1": {
                "type": "string",
                "comment": ""
                },
                "hs_latest_source_data_2": {
                "type": "string",
                "comment": ""
                },
                "hs_latest_source_timestamp": {
                "type": "datetime",
                "comment": ""
                },
                "currentlyinworkflow": {
                "type": "enumeration",
                "comment": ""
                },
                "engagements_last_meeting_booked": {
                "type": "datetime",
                "comment": ""
                },
                "engagements_last_meeting_booked_campaign": {
                "type": "string",
                "comment": ""
                },
                "engagements_last_meeting_booked_medium": {
                "type": "string",
                "comment": ""
                },
                "engagements_last_meeting_booked_source": {
                "type": "string",
                "comment": ""
                },
                "first_conversion_date": {
                "type": "datetime",
                "comment": ""
                },
                "first_conversion_event_name": {
                "type": "string",
                "comment": ""
                },
                "hs_content_membership_notes": {
                "type": "string",
                "comment": ""
                },
                "hs_content_membership_status": {
                "type": "enumeration",
                "comment": ""
                },
                "hs_last_sales_activity_timestamp": {
                "type": "datetime",
                "comment": ""
                },
                "hs_sales_email_last_clicked": {
                "type": "datetime",
                "comment": ""
                },
                "hs_sales_email_last_opened": {
                "type": "datetime",
                "comment": ""
                },
                "hs_sales_email_last_replied": {
                "type": "datetime",
                "comment": ""
                },
                "hs_sequences_is_enrolled": {
                "type": "bool",
                "comment": ""
                },
                "message": {
                "type": "string",
                "comment": ""
                },
                "notes_last_contacted": {
                "type": "datetime",
                "comment": ""
                },
                "notes_last_updated": {
                "type": "datetime",
                "comment": ""
                },
                "notes_next_activity_date": {
                "type": "datetime",
                "comment": ""
                },
                "num_contacted_notes": {
                "type": "number",
                "comment": ""
                },
                "num_notes": {
                "type": "number",
                "comment": ""
                },
                "address": {
                "type": "string",
                "comment": ""
                },
                "annualrevenue": {
                "type": "string",
                "comment": ""
                },
                "city": {
                "type": "string",
                "comment": ""
                },
                "company": {
                "type": "string",
                "comment": ""
                },
                "country": {
                "type": "string",
                "comment": ""
                },
                "createdate": {
                "type": "datetime",
                "comment": ""
                },
                "email": {
                "type": "string",
                "comment": ""
                },
                "fax": {
                "type": "string",
                "comment": ""
                },
                "firstname": {
                "type": "string",
                "comment": ""
                },
                "hs_content_membership_email_confirmed": {
                "type": "bool",
                "comment": ""
                },
                "hs_content_membership_registered_at": {
                "type": "datetime",
                "comment": ""
                },
                "hs_content_membership_registration_domain_sent_to": {
                "type": "string",
                "comment": ""
                },
                "hs_content_membership_registration_email_sent_at": {
                "type": "datetime",
                "comment": ""
                },
                "hs_created_by_user_id": {
                "type": "number",
                "comment": ""
                },
                "hs_createdate": {
                "type": "datetime",
                "comment": ""
                },
                "hs_email_domain": {
                "type": "string",
                "comment": ""
                },
                "hs_language": {
                "type": "enumeration",
                "comment": ""
                },
                "hs_latest_sequence_ended_date": {
                "type": "datetime",
                "comment": ""
                },
                "hs_latest_sequence_enrolled": {
                "type": "number",
                "comment": ""
                },
                "hs_latest_sequence_enrolled_date": {
                "type": "datetime",
                "comment": ""
                },
                "hs_lifecyclestage_customer_date": {
                "type": "datetime",
                "comment": ""
                },
                "hs_lifecyclestage_evangelist_date": {
                "type": "datetime",
                "comment": ""
                },
                "hs_lifecyclestage_lead_date": {
                "type": "datetime",
                "comment": ""
                },
                "hs_lifecyclestage_marketingqualifiedlead_date": {
                "type": "datetime",
                "comment": ""
                },
                "hs_lifecyclestage_opportunity_date": {
                "type": "datetime",
                "comment": ""
                },
                "hs_lifecyclestage_other_date": {
                "type": "datetime",
                "comment": ""
                },
                "hs_lifecyclestage_salesqualifiedlead_date": {
                "type": "datetime",
                "comment": ""
                },
                "hs_lifecyclestage_subscriber_date": {
                "type": "datetime",
                "comment": ""
                },
                "hs_merged_object_ids": {
                "type": "enumeration",
                "comment": ""
                },
                "hs_object_id": {
                "type": "number",
                "comment": ""
                },
                "hs_persona": {
                "type": "enumeration",
                "comment": ""
                },
                "hs_sequences_enrolled_count": {
                "type": "number",
                "comment": ""
                },
                "hs_timezone": {
                "type": "enumeration",
                "comment": ""
                },
                "value": {
                "type": "string",
                "comment": ""
                },
                "hs_whatsapp_phone_number": {
                "type": "string",
                "comment": ""
                },
                "industry": {
                "type": "string",
                "comment": ""
                },
                "jobtitle": {
                "type": "string",
                "comment": ""
                },
                "lastmodifieddate": {
                "type": "datetime",
                "comment": ""
                },
                "lastname": {
                "type": "string",
                "comment": ""
                },
                "lifecyclestage": {
                "type": "enumeration",
                "comment": ""
                },
                "mobilephone": {
                "type": "string",
                "comment": ""
                },
                "numemployees": {
                "type": "enumeration",
                "comment": ""
                },
                "phone": {
                "type": "string",
                "comment": ""
                },
                "salutation": {
                "type": "string",
                "comment": ""
                },
                "state": {
                "type": "string",
                "comment": ""
                },
                "twitterhandle": {
                "type": "string",
                "comment": ""
                },
                "website": {
                "type": "string",
                "comment": ""
                },
                "zip": {
                "type": "string",
                "comment": ""
                },
                "hs_facebook_click_id": {
                "type": "string",
                "comment": ""
                },
                "hs_google_click_id": {
                "type": "string",
                "comment": ""
                },
                "hs_ip_timezone": {
                "type": "string",
                "comment": ""
                },
                "ip_city": {
                "type": "string",
                "comment": ""
                },
                "ip_country": {
                "type": "string",
                "comment": ""
                },
                "ip_country_code": {
                "type": "string",
                "comment": ""
                },
                "ip_state": {
                "type": "string",
                "comment": ""
                },
                "ip_state_code": {
                "type": "string",
                "comment": ""
                },
                "num_conversion_events": {
                "type": "number",
                "comment": ""
                },
                "num_unique_conversion_events": {
                "type": "number",
                "comment": ""
                },
                "recent_conversion_date": {
                "type": "datetime",
                "comment": ""
                },
                "recent_conversion_event_name": {
                "type": "string",
                "comment": ""
                },
                "closedate": {
                "type": "datetime",
                "comment": ""
                },
                "days_to_close": {
                "type": "number",
                "comment": ""
                },
                "first_deal_created_date": {
                "type": "datetime",
                "comment": ""
                },
                "hs_buying_role": {
                "type": "enumeration",
                "comment": ""
                },
                "num_associated_deals": {
                "type": "number",
                "comment": ""
                },
                "recent_deal_amount": {
                "type": "number",
                "comment": ""
                },
                "recent_deal_close_date": {
                "type": "datetime",
                "comment": ""
                },
                "total_revenue": {
                "type": "number",
                "comment": ""
                },
                "hs_email_bad_address": {
                "type": "bool",
                "comment": ""
                },
                "hs_email_bounce": {
                "type": "number",
                "comment": ""
                },
                "hs_email_click": {
                "type": "number",
                "comment": ""
                },
                "hs_email_customer_quarantined_reason": {
                "type": "enumeration",
                "comment": ""
                },
                "hs_email_delivered": {
                "type": "number",
                "comment": ""
                },
                "hs_email_first_click_date": {
                "type": "datetime",
                "comment": ""
                },
                "hs_email_first_open_date": {
                "type": "datetime",
                "comment": ""
                },
                "hs_email_first_reply_date": {
                "type": "datetime",
                "comment": ""
                },
                "hs_email_first_send_date": {
                "type": "datetime",
                "comment": ""
                },
                "hs_email_hard_bounce_reason_enum": {
                "type": "enumeration",
                "comment": ""
                },
                "hs_email_last_click_date": {
                "type": "datetime",
                "comment": ""
                },
                "hs_email_last_email_name": {
                "type": "string",
                "comment": ""
                },
                "hs_email_last_open_date": {
                "type": "datetime",
                "comment": ""
                },
                "hs_email_last_reply_date": {
                "type": "datetime",
                "comment": ""
                },
                "hs_email_last_send_date": {
                "type": "datetime",
                "comment": ""
                },
                "hs_email_open": {
                "type": "number",
                "comment": ""
                },
                "hs_email_optout": {
                "type": "bool",
                "comment": ""
                },
                "hs_email_optout_85451665": {
                "type": "enumeration",
                "comment": ""
                },
                "hs_email_quarantined": {
                "type": "bool",
                "comment": ""
                },
                "hs_email_quarantined_reason": {
                "type": "Enumeration",
                "comment": ""
                },
                "hs_email_replied": {
                "type": "Number",
                "comment": ""
                },
                "hs_email_sends_since_last_engagement": {
                "type": "Number",
                "comment": ""
                },
                "hs_emailconfirmationstatus": {
                "type": "Enumeration",
                "comment": ""
                },
                "hs_legal_basis": {
                "type": "Enumeration",
                "comment": ""
                },
                "company_size": {
                "type": "String",
                "comment": ""
                },
                "date_of_birth": {
                "type": "String",
                "comment": ""
                },
                "degree": {
                "type": "String",
                "comment": ""
                },
                "field_of_study": {
                "type": "String",
                "comment": ""
                },
                "gender": {
                "type": "String",
                "comment": ""
                },
                "graduation_date": {
                "type": "String",
                "comment": ""
                },
                "job_function": {
                "type": "String",
                "comment": ""
                },
                "marital_status": {
                "type": "String",
                "comment": ""
                },
                "military_status": {
                "type": "String",
                "comment": ""
                },
                "relationship_status": {
                "type": "String",
                "comment": ""
                },
                "school": {
                "type": "String",
                "comment": ""
                },
                "seniority": {
                "type": "String",
                "comment": ""
                },
                "start_date": {
                "type": "String",
                "comment": ""
                },
                "work_email": {
                "type": "String",
                "comment": ""
                },
                "hs_is_unworked": {
                "type": "Boolean",
                "comment": ""
                },
                "hs_lead_status": {
                "type": "Enumeration",
                "comment": ""
                },
                "hubspot_owner_assigneddate": {
                "type": "DateTime",
                "comment": ""
                },
                "hubspot_owner_id": {
                "type": "Enumeration",
                "comment": ""
                },
                "hubspot_team_id": {
                "type": "Enumeration",
                "comment": ""
                },
                "hubspotscore": {
                "type": "Number",
                "comment": ""
                }
            }
        },
        {
        "name": "deal",
        "fields": {
        'id':{
               'type':'String',
                'comment':'',
                'isPrimaryKey': true,
                 'primaryKeyPos': 1
            },

"hs_analytics_latest_source": {
"type": "Enumeration",
"comment": ""
},
"hs_analytics_latest_source_data_1": {
"type": "string",
"comment": ""
},
"hs_analytics_latest_source_data_2": {
"type": "string",
"comment": ""
},
"hs_analytics_latest_source_timestamp": {
"type": "string",
"format": "date-time",
"comment": ""
},
"hs_analytics_source": {
"type": "Enumeration",
"comment": ""
},
"hs_analytics_source_data_1": {
"type": "string",
"comment": ""
},
"hs_analytics_source_data_2": {
"type": "string",
"comment": ""
},
"closed_lost_reason": {
"type": "string",
"comment": ""
},
"closed_won_reason": {
"type": "string",
"comment": ""
},
"dealstage": {
"type": "Enumeration",
"comment": ""
},
"engagements_last_meeting_booked": {
"type": "DateTime",
"comment": ""
},
"engagements_last_meeting_booked_campaign": {
"type": "string",
"comment": ""
},
"engagements_last_meeting_booked_medium": {
"type": "string",
"comment": ""
},
"engagements_last_meeting_booked_source": {
"type": "string",
"comment": ""
},
"hs_lastmodifieddate": {
"type": "DateTime",
"comment": ""
},
"hubspot_owner_assigneddate": {
"type": "DateTime",
"comment": ""
},
"notes_last_contacted": {
"type": "DateTime",
"comment": ""
},
"notes_last_updated": {
"type": "DateTime",
"comment": ""
},
"notes_next_activity_date": {
"type": "DateTime",
"comment": ""
},
"num_contacted_notes": {
"type": "number",
"comment": ""
},
"num_notes": {
"type": "number",
"comment": ""
},
"pipeline": {
"type": "Enumeration",
"comment": ""
},
"amount": {
"type": "number",
"comment": ""
},
"amount_in_home_currency": {
"type": "number",
"comment": ""
},
"hs_acv": {
"type": "number",
"comment": ""
},
"hs_arr": {
"type": "number",
"comment": ""
},
"hs_exchange_rate": {
"type": "number",
"comment": ""
},
"hs_mrr": {
        "type": "Number",
        "comment": ""
    },
    "hs_tcv": {
        "type": "Number",
        "comment": ""
    },
    "closedate": {
        "type": "DateTime",
        "comment": ""
    },
    "createdate": {
        "type": "DateTime",
        "comment": ""
    },
    "dealname": {
        "type": "String",
        "comment": ""
    },
    "dealtype": {
        "type": "Enumeration",
        "comment": ""
    },
    "description": {
        "type": "String",
        "comment": ""
    },
    "hs_all_collaborator_owner_ids": {
        "type": "Enumeration",
        "comment": ""
    },
    "hs_created_by_user_id": {
        "type": "Number",
        "comment": ""
    },
    "hs_deal_stage_probability": {
        "type": "Number",
        "comment": ""
    },
    "hs_forecast_amount": {
        "type": "Number",
        "comment": ""
    },
    "hs_forecast_probability": {
        "type": "Number",
        "comment": ""
    },
    "hs_manual_forecast_category": {
        "type": "Enumeration",
        "comment": ""
    },
    "hs_merged_object_ids": {
        "type": "Enumeration",
        "comment": ""
    },
    "hs_next_step": {
        "type": "String",
        "comment": ""
    },
    "hs_object_id": {
        "type": "Number",
        "comment": ""
    },
    "hs_priority": {
        "type": "Enumeration",
        "comment": ""
    },
    "hs_projected_amount": {
        "type": "Number",
        "comment": ""
    },
    "hs_projected_amount_in_home_currency": {
        "type": "Number",
        "comment": ""
    },
    "hubspot_owner_id": {
        "type": "Enumeration",
        "comment": ""
    },
    "hubspot_team_id": {
        "type": "Enumeration",
        "comment": ""
    },
    "num_associated_contacts": {
        "type": "Number",
        "comment": ""
    }
        }
    },
        {
    "name": 'owners',
    "fields":{
    'id':{
          'type':'String',
            'comment':'',
            'isPrimaryKey': true,
            'primaryKeyPos': 1
          },
       'email':{
             'type':'String',
             'comment':'',
             },
        'firstName':{
             'type':'String',
              'comment':'',
              },
         'lastName':{
            'type':'String',
              'comment':'',
             },
         'userId':{
           'type':'Number',
            'comment':'',
             },
        'createdAt':{
             'type':'DateTime',
             'comment':'',
           },
        'updatedAt':{
              'type':'DateTime',
                'comment':'',
             },
        'archived':{
            'type':'Boolean',
             'comment':'',
         },
    }
    }
    ];
}

const tableMapping = {
    'contact': 'getContacts',
    'company': 'getCompanies',
    'deal': 'getDeals',
    'owners': 'getOwners'
}
function batchRead(connectionConfig, nodeConfig, offset, tableName, pageSize, batchReadSender) {
log.warn("Start Batch Read ------------------------------")
    if(!offset || !offset.tableName){
            offset = {
                tableName:tableName
            };
        }
          iterateAllData(tableMapping[tableName], offset, (result, offsetNext, error) => {
                 if(result.results && result.results.length>0){
                    if(result.paging && result.paging.next){
                    offset['after'] = 'after=' + result.paging.next.after
                     }
                  batchReadSender.send(result.results,tableName,offset);
                 if(!result || !result.paging || !result.paging.next){
                       return false
                 }
                 }else{
                    return false
                 }
                    return isAlive()
                });
              batchReadSender.send(offset);
}


function commandCallback(connectionConfig, nodeConfig, commandInfo) {
    if (commandInfo.command === 'OAuth'){
        let obj = {code:connectionConfig.code};
        Object.assign(obj,clientInfo)
        let getToken = invoker.invokeWithoutIntercept("accessToken", obj);
        if(getToken.result){
        let refreshToken = {"refresh_token": getToken.result.refresh_token}
        connectionConfig.refresh_token = getToken.result.refresh_token;
        Object.assign(refreshToken,clientInfo)
        connectionConfig.access_token = getToken.result.access_token;
        }
        return connectionConfig;
    }
    }


function updateToken(connectionConfig, nodeConfig, apiResponse) {

    if (apiResponse.httpCode === 401 || (apiResponse.result && apiResponse.result.length > 0 && apiResponse.result[0].errorCode && apiResponse.result[0].errorCode === 'INVALID_SESSION_ID')) {
        try {
            let getToken = invoker.invokeWithoutIntercept("refreshAccessToken", clientInfo);
            let httpCode = getToken.httpCode
            checkAuthority(getToken, httpCode);
            if (getToken && getToken.result && getToken.result.access_token) {
                connectionConfig.Authorization = getToken.result.access_token;
                return {"access_token": getToken.result.access_token};
            }
        } catch (e) {
            log.warn(e)
            throw(e);
        }
    }
    return null;
}

function connectionTest(connectionConfig) {
    let invoke;
    let httpCode;
    try {
        let getToken = invoker.invokeWithoutIntercept("refreshAccessToken", clientInfo);
        if (getToken && getToken.result && getToken.result.access_token) {
            connectionConfig.access_token = getToken.result.access_token;
            connectionConfig.Authorization = getToken.result.access_token;
         }
        invoke = invoker.invoke('getDeals', {access_token: connectionConfig.access_token});
        httpCode = invoke.httpCode;
        return [
            {
                "test": "Permission check",
                "code": exceptionUtil.statusCode(httpCode),
                "result": result(invoke, httpCode)
            }
        ];
    } catch (e) {
        return [
            {
                "test": "Authorization failed",
                "code": -1,
                "result": exceptionUtil.eMessage(e)
            }
        ];
    }
}

