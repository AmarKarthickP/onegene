# Copyright (c) 2025, TEAMPRO and contributors
# For license information, please see license.txt

import frappe
from frappe.model.document import Document



class GateEntry(Document):
    def before_insert(self):
        if frappe.db.exists('Gate Entry',{'docstatus':['!=',2],'entry_against':self.entry_against,'entry_id':self.entry_id,'name':['!=',self.name]}):
            frappe.throw('Gate entry already created for this document')
          
    def after_insert(self):
        if self.entry_against == 'General DC' and self.entry_id:
            if frappe.db.exists('General DC', {'name': self.entry_id,'workflow_state': ['in', ['Approved', 'DC Received']]}):
                doc = frappe.get_doc('General DC', self.entry_id)
                doc.workflow_state = 'Dispatched'
                doc.save(ignore_permissions=True)
        elif self.entry_against=='Sales Invoice' and self.entry_id:
            if frappe.db.exists('Sales Invoice', {'name':self.entry_id}):
                doc = frappe.get_doc('Sales Invoice', self.entry_id)
                if doc.workflow_state=='Approved':
                    doc.custom_gate_entry_completed=1
                    frappe.db.sql("""
                        UPDATE `tabSales Invoice`
                        SET workflow_state = %s
                        WHERE name = %s AND docstatus = 1
                    """, ('Dispatched', self.entry_id))

                    if doc.custom_invoice_type=='Export Invoice':
                        if frappe.db.exists('Logistics Request',{'status':'Ready to Ship','order_no':self.entry_id}):
                            lr=frappe.get_doc('Logistics Request',{'status':'Ready to Ship','order_no':self.entry_id})
                            lr.status='Dispatched'
                            lr.save(ignore_permissions=True)
        elif self.entry_against=='Advance Shipping Note':
            if frappe.db.exists('Advance Shipping Note', {'name':self.entry_id}):
                doc = frappe.get_doc('Advance Shipping Note', self.entry_id)
                if doc.workflow_state=='In Transit':
                    doc.workflow_state='Gate Received'
                    doc.confirm_supplier_dn=self.ref_no
                    doc.security_name=self.security_name
                    doc.vehicle_no=self.vehicle_number
                    doc.driver_name=self.driver_name
                    doc.received_date_time=self.entry_time
                doc.save(ignore_permission=True)
       
                
        self.submit()


