import frappe
import requests
from datetime import date
import erpnext
import json
import frappe
from itertools import groupby
from operator import itemgetter

from datetime import time
from frappe.utils import money_in_words
import urllib.parse
from frappe.utils import now
from frappe import throw,_, bold
from frappe.utils import flt
from dateutil.relativedelta import relativedelta
from frappe.utils import now_datetime
from frappe.utils import format_time, formatdate, now
from frappe.model.naming import make_autoname
from erpnext.setup.utils import get_exchange_rate
from frappe.model.workflow import apply_workflow
from frappe.utils import (
	add_days,
	ceil,
	cint,
	comma_and,
	flt,
	get_link_to_form,
	getdate,
	now_datetime,
	datetime,get_first_day,get_last_day,
	nowdate,
	today,
)
from frappe.utils import cstr, cint, getdate, get_last_day, get_first_day, add_days,date_diff
from datetime import date, datetime, timedelta
import datetime as dt
from datetime import datetime, timedelta
from typing import Dict, Optional, Tuple, Union
from datetime import datetime
from onegene.mark_attendance import check_holiday
from frappe.utils.password import update_password
from frappe.utils.background_jobs import enqueue
import datetime as dt
from datetime import datetime, timedelta
from frappe.contacts.doctype.address.address import (
	get_address_display
)









@frappe.whitelist()

def item_price_revision_iom(docname):
    
    iom_doc = frappe.get_doc("Inter Office Memo",docname)
    
    if( (iom_doc.iom_type =="Approval for Price Revision SO" and iom_doc.price_revision_po) or(iom_doc.iom_type =="Approval for Price Revision PO" and iom_doc.document=="Sales Order" and iom_doc.price_revision_po) ):
        
        
        
            
        iom_so = frappe.db.sql("""
                            
                            SELECT DISTINCT po_no 
                            FROM `tabApproval for Price Revision PO`
                            WHERE parent = %s
                            
                            """,(docname,), as_dict=True)
        
        for so in iom_so:
            i_so = so.po_no
            op_doc = frappe.get_doc("Open Order",{"sales_order_number":i_so})
            
            if op_doc.open_order_table:
                
                for row in iom_doc.price_revision_po:
                    if row.po_no == i_so:

                        for op_row in op_doc.open_order_table:
                            if op_row.item_code == row.part_no:
                                op_row.rate = row.new_price
                                
                                sos_doc = frappe.get_doc("Sales Order Schedule",{"Item_code":row.part_no,"sales_order_number":i_so})
                                
                                if sos_doc:
                                    
                                    order_rate = row.new_price
                                    frappe.db.set_value("Sales Order Schedule",sos_doc.name,"order_rate",order_rate)
                                    
                                    order_rate_inr = sos_doc.exchange_rate * row.new_price
                                    frappe.db.set_value("Sales Order Schedule",sos_doc.name,"order_rate_inr",order_rate_inr)
                                    
                                    schedule_amount = sos_doc.qty * row.new_price
                                    frappe.db.set_value("Sales Order Schedule",sos_doc.name,"schedule_amount",schedule_amount)
                                    
                                    schedule_amount_inr = sos_doc.qty * row.new_price * sos_doc.exchange_rate
                                    frappe.db.set_value("Sales Order Schedule",sos_doc.name,"schedule_amount_inr",schedule_amount_inr)
                                    
                                    delivered_amount = sos_doc.delivered_qty * row.new_price
                                    frappe.db.set_value("Sales Order Schedule",sos_doc.name,"delivered_amount",delivered_amount)
                                    
                                    delivered_amount_inr = sos_doc.delivered_qty * row.new_price * sos_doc.exchange_rate
                                    frappe.db.set_value("Sales Order Schedule",sos_doc.name,"delivered_amount_inr",delivered_amount_inr)
                                    
                                    pending_amount = sos_doc.pending_qty * row.new_price
                                    frappe.db.set_value("Sales Order Schedule",sos_doc.name,"pending_amount",pending_amount)
                                    
                                    pending_amount_inr = sos_doc.pending_qty * row.new_price * sos_doc.exchange_rate
                                    frappe.db.set_value("Sales Order Schedule",sos_doc.name,"pending_amount_inr",pending_amount_inr)
                                
                                
                                
                                break
                    
                op_doc.disable_update_items=0                
                op_doc.save(ignore_permissions=True)
                
        frappe.db.commit()        
        return True 
    
    
    elif iom_doc.iom_type =="Approval for Price Revision JO" and iom_doc.price_revision_jo:
        
        iom_so = frappe.db.sql("""
                            
                            SELECT DISTINCT po_no 
                            FROM `tabApproval for Price Revision JO`
                            WHERE parent = %s
                            
                            """,(docname,), as_dict=True)
        
        for so in iom_so:
            i_so = so.po_no
            op_doc = frappe.get_doc("Purchase Open Order",{"purchase_order":i_so})
            
            if op_doc.open_order_table:
                
                for row in iom_doc.price_revision_jo :
                    if row.po_no == i_so:

                        for op_row in op_doc.open_order_table:
                            if op_row.item_code == row.part_no:
                                op_row.rate = row.new_price
                                
                                pos_doc = frappe.get_doc("Purchase Order Schedule",{"item_code":row.part_no,"purchase_order_number":i_so})
                                
                                if pos_doc:
                                    
                                    order_rate = row.new_price
                                    frappe.db.set_value("Purchase Order Schedule",pos_doc.name,"order_rate",order_rate)
                                    
                                    order_rate_inr = pos_doc.exchange_rate * row.new_price
                                    frappe.db.set_value("Purchase Order Schedule",pos_doc.name,"order_rate_inr",order_rate_inr)
                                    
                                    schedule_amount = pos_doc.qty * row.new_price
                                    frappe.db.set_value("Purchase Order Schedule",pos_doc.name,"schedule_amount",schedule_amount)
                                    
                                    schedule_amount_inr = pos_doc.qty * row.new_price * pos_doc.exchange_rate
                                    frappe.db.set_value("Purchase Order Schedule",pos_doc.name,"schedule_amount_inr",schedule_amount_inr)
                                    
                                    received_amount = pos_doc.received_qty * row.new_price
                                    frappe.db.set_value("Purchase Order Schedule",pos_doc.name,"received_amount",received_amount)
                                    
                                    received_amount_inr = pos_doc.received_qty * row.new_price * pos_doc.exchange_rate
                                    frappe.db.set_value("Purchase Order Schedule",pos_doc.name,"received_amount_inr",received_amount_inr)
                                    
                                    pending_amount = pos_doc.pending_qty * row.new_price
                                    frappe.db.set_value("Purchase Order Schedule",pos_doc.name,"pending_amount",pending_amount)
                                    
                                    pending_amount_inr = pos_doc.pending_qty * row.new_price * pos_doc.exchange_rate
                                    frappe.db.set_value("Purchase Order Schedule",pos_doc.name,"pending_amount_inr",pending_amount_inr)
                                
                                
                                
                                break
                    
                op_doc.disable_update_items=0                
                op_doc.save(ignore_permissions=True)
                
        frappe.db.commit()        
        return True
    
    elif iom_doc.iom_type =="Approval for Price Revision PO" and iom_doc.custom_approval_for_price_revision_po_new and iom_doc.document=="Purchase Order":
        
    
        iom_so = frappe.db.sql("""
                            
                            SELECT DISTINCT po_no 
                            FROM `tabApproval for Price Revision PO NEW`
                            WHERE parent = %s
                            
                            """,(docname,), as_dict=True)
        
        for so in iom_so:
            i_so = so.po_no
            op_doc = frappe.get_doc("Purchase Open Order",{"purchase_order":i_so})
            
            if op_doc.open_order_table:
                
                for row in iom_doc.custom_approval_for_price_revision_po_new :
                    if row.po_no == i_so:

                        for op_row in op_doc.open_order_table:
                            if op_row.item_code == row.part_no:
                                op_row.rate = row.new_price
                                
                                pos_doc = frappe.get_doc("Purchase Order Schedule",{"item_code":row.part_no,"purchase_order_number":i_so})
                                
                                if pos_doc:
                                    
                                    order_rate = row.new_price
                                    frappe.db.set_value("Purchase Order Schedule",pos_doc.name,"order_rate",order_rate)
                                    
                                    order_rate_inr = pos_doc.exchange_rate * row.new_price
                                    frappe.db.set_value("Purchase Order Schedule",pos_doc.name,"order_rate_inr",order_rate_inr)
                                    
                                    schedule_amount = pos_doc.qty * row.new_price
                                    frappe.db.set_value("Purchase Order Schedule",pos_doc.name,"schedule_amount",schedule_amount)
                                    
                                    schedule_amount_inr = pos_doc.qty * row.new_price * pos_doc.exchange_rate
                                    frappe.db.set_value("Purchase Order Schedule",pos_doc.name,"schedule_amount_inr",schedule_amount_inr)
                                    
                                    received_amount = pos_doc.received_qty * row.new_price
                                    frappe.db.set_value("Purchase Order Schedule",pos_doc.name,"received_amount",received_amount)
                                    
                                    received_amount_inr = pos_doc.received_qty * row.new_price * pos_doc.exchange_rate
                                    frappe.db.set_value("Purchase Order Schedule",pos_doc.name,"received_amount_inr",received_amount_inr)
                                    
                                    pending_amount = pos_doc.pending_qty * row.new_price
                                    frappe.db.set_value("Purchase Order Schedule",pos_doc.name,"pending_amount",pending_amount)
                                    
                                    pending_amount_inr = pos_doc.pending_qty * row.new_price * pos_doc.exchange_rate
                                    frappe.db.set_value("Purchase Order Schedule",pos_doc.name,"pending_amount_inr",pending_amount_inr)
                                
                                
                                
                                break
                    
                op_doc.disable_update_items=0                
                op_doc.save(ignore_permissions=True)
                
        frappe.db.commit()        
        return True                        
                
                