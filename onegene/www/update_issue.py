import frappe

@frappe.whitelist(allow_guest=True)
def update_issue_from_teampro(**args):
	frappe.log_error(message=f"Error :{args}",title=("Issue"))
	if frappe.db.exists("Issue",{'custom_task_id':args['name']}):
		issue_doc=frappe.get_doc("Issue",{'custom_task_id':args['name']})
		issue_doc.subject = args['subject']
		issue_doc.custom_task_id = args['name']
		issue_doc.custom_issue_id = args['issue_id']

		issue_doc.description = args['description']
		issue_doc.priority = args['priority']
		issue_doc.custom_item_group = 'TEAMPRO'
		if args['status']=='Open':
			issue_doc.status = 'Open'
		elif args['status'] in ['Working','Pending Review','Overdue','Code Review','Client Review']:
			issue_doc.status = 'Replied'
		elif args['status']=='Completed':
			issue_doc.status = 'Resolved'
		elif args['status']=='Cancelled':
			issue_doc.status = 'Closed'
		elif args['status']=='Hold':
			issue_doc.status = 'On Hold'
		if args['status']=='Client Review':
			issue_doc.resolution_details = args['pr_remarks']
			issue_doc.custom_proof = 'https://erp.teamproit.com/'+args['proof'] 
		issue_doc.custom_task_allocated_to = args['allocated_to']
		issue_doc.custom_task_creation_date = str(args['creation'])
		# issue_doc.college_name = str(args['cname'])
		# issue_doc.university_name = str(args['uname'])
		# issue_doc.degree = str(args['degree'])
		# issue_doc.specialization = str(args['special'])
	
		issue_doc.save(ignore_permissions=True)

		frappe.db.commit()
		return True
		
	elif frappe.db.exists("Issue",{'custom_issue_id':args['issue_id']}):
		issue_doc=frappe.get_doc("Issue",{'custom_issue_id':args['issue_id']})
		issue_doc.subject = args['subject']
		issue_doc.description = args['description']
		issue_doc.custom_task_id = args['name']
		issue_doc.custom_issue_id = args['issue_id']

		issue_doc.priority = args['priority']
		issue_doc.custom_item_group = 'TEAMPRO'
		if args['status']=='Open':
			issue_doc.status = 'Open'
		elif args['status'] in ['Working','Pending Review','Overdue','Code Review','Client Review']:
			issue_doc.status = 'Replied'
		elif args['status']=='Completed':
			issue_doc.status = 'Resolved'
		elif args['status']=='Cancelled':
			issue_doc.status = 'Closed'
		elif args['status']=='Hold':
			issue_doc.status = 'On Hold'
		if args['status']=='Client Review':
			issue_doc.resolution_details = args['pr_remarks']
			issue_doc.custom_proof = 'https://erp.teamproit.com/'+args['proof']
		issue_doc.custom_task_allocated_to = args['allocated_to']
		issue_doc.custom_task_creation_date = str(args['creation'])
		issue_doc.save(ignore_permissions=True)
		frappe.db.commit()
		return True
	else:
		issue_doc=frappe.new_doc("Issue")
		issue_doc.subject = args['subject']
		issue_doc.custom_task_id = args['name']
		issue_doc.custom_issue_id = args['issue_id']

		issue_doc.description = args['description']
		issue_doc.priority = args['priority']
		issue_doc.custom_item_group = 'TEAMPRO'
		if args['status']=='Open':
			issue_doc.status = 'Open'
		elif args['status'] in ['Working','Pending Review','Overdue','Code Review','Client Review']:
			issue_doc.status = 'Replied'
		elif args['status']=='Completed':
			issue_doc.status = 'Resolved'
		elif args['status']=='Cancelled':
			issue_doc.status = 'Closed'
		elif args['status']=='Hold':
			issue_doc.status = 'On Hold'
		if args['status']=='Client Review':
			issue_doc.resolution_details = args['pr_remarks']
			issue_doc.custom_proof = 'https://erp.teamproit.com/'+args['proof']
		issue_doc.custom_task_allocated_to = args['allocated_to']
		issue_doc.custom_task_creation_date = str(args['creation'])
		issue_doc.save(ignore_permissions=True)
		frappe.db.commit()
		return True
	
@frappe.whitelist()
def update_issueid_from_teampro(**args):
	if frappe.db.exists("Issue",{'subject':args['subject']}):
		issue_doc=frappe.get_doc("Issue",{'subject':args['subject']})
		issue_doc.subject = args['subject']
		issue_doc.custom_issue_id = args['name']
		issue_doc.custom_item_group = 'TEAMPRO'
		issue_doc.status = args['status']
		
		issue_doc.save(ignore_permissions=True)

		frappe.db.commit()
		return True