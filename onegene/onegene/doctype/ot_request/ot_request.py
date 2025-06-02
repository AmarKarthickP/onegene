# Copyright (c) 2024, TEAMPRO and contributors
# For license information, please see license.txt

import frappe
from frappe.model.document import Document
from time import strptime
from datetime import date, timedelta,time
from frappe.utils import today,get_first_day, get_last_day, add_days

class OTRequest(Document):
    # Update the OT hours in attendance and OT hours in OT Balance
    def on_submit(self):    
        if self.workflow_state=='Approved':
            for i in self.employee_details:
                month_start=get_first_day(self.ot_requested_date)
                month_end=get_last_day(self.ot_requested_date)
                name = frappe.db.get_value("Attendance",{"attendance_date":self.ot_requested_date,"docstatus":("!=",2),"employee":i.employee_code,"custom_ot_balance_updated":0},["name"])
                if name:
                    att = frappe.get_doc("Attendance",name)
                    if int(att.custom_extra_hours) >=2:
                        if int(att.custom_extra_hours) >= int(i.requested_ot_hours):
                            ot_hours = time(int(i.requested_ot_hours),0,0)
                        else:
                            ot_hours = time(int(att.custom_extra_hours),0,0)			
                        ftr = [3600,60,1]
                        hr = sum([a*b for a,b in zip(ftr, map(int,str(ot_hours).split(':')))])
                        ot_hr = round(hr/3600,1)
                        frappe.db.set_value('Attendance',name,'custom_total_overtime_hours',ot_hours)
                        frappe.db.set_value('Attendance',name,'custom_overtime_hours',ot_hr)
                        frappe.db.set_value('OT Request',self.name,'ot_updated',True)
                        frappe.db.set_value('Attendance',name,'custom_ot_updated',True)
                        if not frappe.db.exists("OT Balance",{'employee':i.employee_code,'from_date':month_start,'to_date':month_end}):
                            otb=frappe.new_doc("OT Balance")
                            otb.employee=i.employee_code
                            otb.from_date=month_start
                            otb.to_date=month_end
                            otb.total_ot_hours = ot_hr
                            draft=frappe.db.count("Leave Application",{'employee':i.employee_code,'from_date':('between',[month_start,month_end]),'to_date':('between',[month_start,month_end]),'workflow_state':'Draft','custom_select_leave_type':'Comp-off from OT','docstatus':('!=',2)})
                            approved=frappe.db.count("Leave Application",{'employee':i.employee_code,'from_date':('between',[month_start,month_end]),'to_date':('between',[month_start,month_end]),'workflow_state':'Approved','custom_select_leave_type':'Comp-off from OT','docstatus':('!=',2)})
                            otb.comp_off_pending_for_approval = draft
                            otb.comp_off_used = approved
                            otb.ot_balance = otb.total_ot_hours - ((draft * 8)+(approved * 8))
                            otb.save(ignore_permissions=True)
                        else:
                            otb=frappe.get_doc("OT Balance",{'employee':i.employee_code,'from_date':month_start,'to_date':month_end})
                            otb.total_ot_hours += ot_hr
                            draft=frappe.db.count("Leave Application",{'employee':i.employee_code,'from_date':('between',[month_start,month_end]),'to_date':('between',[month_start,month_end]),'workflow_state':'Draft','custom_select_leave_type':'Comp-off from OT','docstatus':('!=',2)})
                            approved=frappe.db.count("Leave Application",{'employee':i.employee_code,'from_date':('between',[month_start,month_end]),'to_date':('between',[month_start,month_end]),'workflow_state':'Approved','custom_select_leave_type':'Comp-off from OT','docstatus':('!=',2)})
                            otb.comp_off_pending_for_approval = draft
                            otb.comp_off_used = approved
                            otb.ot_balance = otb.total_ot_hours - ((draft * 8)+(approved * 8))
                            otb.save(ignore_permissions=True)
                        frappe.db.set_value('Attendance',name,'custom_ot_balance_updated',True)
                        frappe.db.set_value('OT Request',self.name,'ot_balance',True)

# Return Employees based on the Department selected
@frappe.whitelist()
def get_employees(dept=None,category=None):
    emp_list = []
    if category and dept:
        employees = frappe.db.sql("""
            SELECT name, employee_name, designation
            FROM `tabEmployee` 
            WHERE `status` = 'Active' 
            AND `department` = %s 
            AND `employee_category` = %s
            AND `employee_category` NOT IN ('Staff', 'Sub Staff')
        """, (dept,category), as_dict=1)
        for emp in employees:
            emp_list.append({
                'name': emp.name,
                'employee_name': emp.employee_name,
                'designation': emp.designation
            })
    elif dept and not category:
        employees = frappe.db.sql("""
            SELECT name, employee_name, designation
            FROM `tabEmployee` 
            WHERE `status` = 'Active' 
            AND `department` = %s 
            AND `employee_category` NOT IN ('Staff', 'Sub Staff')
        """, (dept,), as_dict=1)
        for emp in employees:
            emp_list.append({
                'name': emp.name,
                'employee_name': emp.employee_name,
                'designation': emp.designation
            })
    
    return emp_list
@frappe.whitelist()
#Return the Employee Name and Designation by passing the Employee Code
def get_details(name,dep=None):
    dept=frappe.db.get_value("Employee", {'name': name}, ['department'])
    if dep:
        dept=frappe.db.get_value("Employee", {'name': name}, ['department'])
        if dept!=dep:
            return 'ok'
    if name:
        category=frappe.db.get_value("Employee", {'name': name}, ['employee_category'])
        if category=='Staff' or category=='Sub Staff':
            # frappe.throw("Staff category not allowed for OT request")
            return 'OK'
        else:
            emp = frappe.db.get_value("Employee", {'name': name}, ['employee_name', 'designation'])
            return emp  
    
