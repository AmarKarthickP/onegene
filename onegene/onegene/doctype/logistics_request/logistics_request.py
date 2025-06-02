# Copyright (c) 2025, TEAMPRO and contributors
# For license information, please see license.txt

import frappe
from frappe import throw, _, scrub
from frappe.model.document import Document
from erpnext.setup.utils import get_exchange_rate
from frappe.utils import get_url_to_form, today, add_days, nowdate, flt, getdate
from frappe.core.api.file import zip_files
import json
from frappe.model.mapper import get_mapped_doc
from frappe.custom.doctype.property_setter.property_setter import make_property_setter

class LogisticsRequest(Document):
    def validate(self):
        update_workflow(self)

    @frappe.whitelist()
    def compare_po_items(self):
        if self.po_so == 'Purchase Order':
            multiple_pos_list = [po.strip() for po in self.multiple_pos.split(',')] if self.multiple_pos else []
            net_weight = 0
            gross_weight = 0
            for item in self.product_description:
                if multiple_pos_list:
                    actual_qty = frappe.db.get_value('Purchase Order Item',{'parent':('in',multiple_pos_list),'item_code':item.item_code,'material_request':item.material_request},'qty')
                    utilized_qty = frappe.db.sql("""select `tabPurchase Order Item`.qty as qty from `tabLogistics Request`
                    left join `tabPurchase Order Item` on `tabLogistics Request`.name = `tabPurchase Order Item`.parent where `tabPurchase Order Item`.item_code = '%s' and `tabLogistics Request`.name != '%s' and `tabPurchase Order Item`.parent = '%s' and `tabLogistics Request`.docstatus != 2 """%(item.item_code,self.name,item.parent),as_dict=True)
                else:
                    actual_qty = frappe.db.get_value('Purchase Order Item',{'parent':self.order_no,'item_code':item.item_code,'material_request':item.material_request},'qty')
                    utilized_qty = frappe.db.sql("""select `tabPurchase Order Item`.qty as qty from `tabLogistics Request`
                    left join `tabPurchase Order Item` on `tabLogistics Request`.name = `tabPurchase Order Item`.parent where `tabPurchase Order Item`.item_code = '%s' and `tabLogistics Request`.name != '%s' and `tabLogistics Request`.order_no = '%s' and `tabLogistics Request`.docstatus != 2 """%(item.item_code,self.name,self.order_no),as_dict=True)
                
                if not utilized_qty:
                    utilized_qty = 0
                else:
                    utilized_qty = utilized_qty[0].qty
                remaining_qty = int(actual_qty) - utilized_qty
                if item.qty > remaining_qty:
                    msg = """<table class='table table-bordered'><tr><th>Purchase Order Qty</th><td>%s</td></tr>
                    <tr><th>Logistics Request Already raised for</th><td>%s</td></tr>
                    <tr><th>Remaining Qty</th><td>%s</td></tr>
                    </table><p><b>Requesting Qty should not go beyond Remaining Qty</b><p>"""%(actual_qty,utilized_qty,remaining_qty)
                    return msg
            

    

@frappe.whitelist()
def get_supporting_docs(selected_docs):
    selected_docs = json.loads(selected_docs)
    file_list = []
    for s in selected_docs:
        file_name = frappe.get_value("File", {"file_url": s['attach']},"name")
        file_list.append(file_name)
    return file_list

@frappe.whitelist()
def make_purchase_order(source_name, target_doc=None, args=None):
    pos=[]
    if args is None:
        args = {}
    if isinstance(args, str):
        args = json.loads(args)

    def postprocess(source, target_doc):
        
        if frappe.flags.args and frappe.flags.args.default_supplier:
            # items only for given default supplier
            supplier_items = []
            for d in target_doc.items:
                default_supplier = get_item_defaults(d.item_code, target_doc.company).get("default_supplier")
                if frappe.flags.args.default_supplier == default_supplier:
                    supplier_items.append(d)
            target_doc.items = supplier_items
        
        target_doc.logistic_type='Import'
        target_doc.po_so='Purchase Order'
        if target_doc.multiple_pos:
            # If there are already values, append a separator (comma, for example)
            target_doc.multiple_pos += ", " + source.name
        else:
            # If no values, just set it to the first PO name
            target_doc.multiple_pos = source.name

    def select_item(d):
        filtered_items = args.get("filtered_children", [])
        child_filter = d.name in filtered_items if filtered_items else True

        return d.ordered_qty < d.stock_qty and child_filter
    # current_date = datetime.strptime(nowdate(), "%Y-%m-%d").date()
    # frappe.errprint(type(current_date))
    doclist = get_mapped_doc(
        "Purchase Order",
        source_name,
        {
            "Purchase Order": {
                "doctype": "Purchase Order",
                "validation": {"docstatus": ["=", 1]},
            },
            "Purchase Order Item": {
                "doctype": "Purchase Order Item",
                "field_map": [
                    ["name", "purchase_order_item"],
                    ["parent", "purchase_order"],
                    ["uom", "stock_uom"],
                    ["uom", "uom"],
                    ["sales_order", "sales_order"],
                    ["sales_order_item", "sales_order_item"],
                    ["wip_composite_asset", "wip_composite_asset"],
                    ["material_request", "material_request"],
                    ["material_request_item", "material_request_item"],
                    ['schedule_date','schedule_date']
                ],
                "postprocess": update_item,
                "condition": select_item,
            },
        },
        target_doc,
        postprocess,
    )

    return doclist

def update_item(obj, target, source_parent):
    target.conversion_factor = obj.conversion_factor
    target.qty = flt(flt(obj.stock_qty) - flt(obj.ordered_qty)) / target.conversion_factor
    target.stock_qty = target.qty * target.conversion_factor
    if getdate(target.schedule_date) < getdate(nowdate()):
        target.schedule_date = getdate(nowdate())

@frappe.whitelist()
def set_property():
    make_property_setter('Sales Order Item', 'custom_schedule_button', "in_list_view", 0, "Check")
    # make_property_setter('Purchase Order Item', 'schedule', "in_list_view", 0, "Check")
    # make_property_setter('Purchase Order Item', 'schedule', "columns", 0, "Int")

@frappe.whitelist()
def set_property_so():
    make_property_setter('Sales Order Item', 'custom_schedule_button', "in_list_view", 1, "Check")

@frappe.whitelist()
def get_filtered_ports(doctype, txt, searchfield, start, page_len, filters):
    cargo_type = filters.get('cargo_type', '')
    data = frappe.db.sql("""
        SELECT name FROM `tabPORT`
        WHERE cargo_type LIKE %s
        AND name LIKE %s
        LIMIT %s OFFSET %s
    """, (
        f"%{cargo_type}%",
        f"%{txt}%",
        page_len,
        start
    ))
    return data

@frappe.whitelist()
def get_box_pallet_summary(sales_invoice):
    # sales_invoice = "SINV-25-00002"
    if frappe.db.exists("Sales Invoice", sales_invoice):
        doc = frappe.get_doc("Sales Invoice", sales_invoice)
        html = """
                <style>
                    th, td {
                        border: 1px solid black;
                        padding-left: 8px;
                        text-align: left;
                        font-size: 12px
                    } 
                </style>
                <p>Summary of Box and Pallet</p>
                <table style="width: 200%; border-collapse: collapse;">
                    <tr>
                        <td style="background-color: #f68b1f; color: white; font-weigt: 500; width: 20%;">Box Name</td>
                        <td style="background-color: #f68b1f; color: white; font-weigt: 500; width: 15%;">Total No. of Boxes</td>
                        <td style="background-color: #f68b1f; color: white; font-weigt: 500; width: 15%;">Weight Per Unit (in Kg)</td>
                        <td style="background-color: #f68b1f; color: white; font-weigt: 500; width: 15%;">Total Weight (in Kg)</td>
                        <td style="background-color: #f68b1f; color: white; font-weigt: 500; width: 10%;">Total Length</td>
                        <td style="background-color: #f68b1f; color: white; font-weigt: 500; width: 15%;">Total Breadth</td>
                        <td style="background-color: #f68b1f; color: white; font-weigt: 500; width: 10%;">Total Height</td>
                    </tr>
            """
        data = frappe.db.sql("""
            SELECT custom_box as box_name, custom_no_of_boxes as total_no, SUM(custom_weight_per_unit_b) as weight_per_unit, SUM(custom_total_weight_of_boxes) as total_weight,
            SUM(custom_box_length) as blength,  
            SUM(custom_box_height) as bheight,            
            SUM(custom_box_breadth) as bbreadth
            FROM `tabSales Invoice Item`
            WHERE parent = %s
            GROUP BY custom_box
        """, (sales_invoice,), as_dict=True)
        for row in data:
            if row.box_name and row.total_no and row.weight_per_unit and row.total_weight:
                # html += "<tr><td>{}</td><td>{}</td><td>{}</td><td>{}</td></tr>".format(row.box_name, row.total_no, row.weight_per_unit, row.total_weight)
                # html += "<tr><td>{}</td><td>{}</td><td>{}</td><td>{}</td></tr>".format(row.box_name, row.total_no, row.weight_per_unit, row.total_weight)
                # html += "<tr><th>Box Name</th><th>Total No. of Boxes</th><th>Weight Per Unit (in Kg)</th><th>Total Weight (in Kg)</th></tr>"
                html += f"""
                            <tr>
                                <td>{row.box_name}</td>
                                <td>{row.total_no}</td>
                                <td>{row.weight_per_unit}</td>
                                <td>{row.total_weight}</td>
                                <td>{row.blength}</td>
                                <td>{row.bbreadth}</td>
                                <td>{row.bheight}</td>
                                
                            </tr>
                        """
        html += "</table>"
        html += """
                <table style="width: 200%; margin-top: 10px; border-collapse: collapse;">
                    <tr>
                        <td style="background-color: #f68b1f; color: white; font-weigt: 500; width: 20%;">Pallete Name</td>
                        <td style="background-color: #f68b1f; color: white; font-weigt: 500; width: 15%;">Total No. of Pallets</td>
                        <td style="background-color: #f68b1f; color: white; font-weigt: 500; width: 15%;">Weight Per Unit (in Kg)</td>
                        <td style="background-color: #f68b1f; color: white; font-weigt: 500; width: 15%;">Total Weight (in Kg)</td>
                        <td style="background-color: #f68b1f; color: white; font-weigt: 500; width: 10%;">Total Length</td>
                        <td style="background-color: #f68b1f; color: white; font-weigt: 500; width: 15%;">Total Breadth</td>
                        <td style="background-color: #f68b1f; color: white; font-weigt: 500; width: 10%;">Total Height</td>
                    </tr>
            """
        data = frappe.db.sql("""
            SELECT custom_pallet as pallet_name, custom_no_of_pallets as total_no, SUM(custom_weight_per_unit_p) as weight_per_unit, SUM(custom_total_weight_of_pallets) as total_weight,
            SUM(custom_pallet_length) as plength,
            SUM(custom_pallet_breadth) as pbreadth,
            SUM(custom_pallet_height) as pheight
            FROM `tabSales Invoice Item`
            WHERE parent = %s
            GROUP BY custom_pallet
        """, (sales_invoice,), as_dict=True)
        for row in data:
            if row.pallet_name and row.total_no and row.weight_per_unit and row.total_weight:
                # html += "<tr><td>{}</td><td>{}</td><td>{}</td><td>{}</td></tr>".format(row.pallet_name, row.total_no, row.weight_per_unit, row.total_weight)
                # html += "<tr><td>{}</td><td>{}</td><td>{}</td><td>{}</td></tr>".format(row.pallet_name, row.total_no, row.weight_per_unit, row.total_weight)
                # html += "<tr><th>Box Name</th><th>Total No. of Boxes</th><th>Weight Per Unit (in Kg)</th><th>Total Weight (in Kg)</th></tr>"
                html += f"""
                            <tr>
                                <td>{row.pallet_name}</td>
                                <td>{row.total_no}</td>
                                <td>{row.weight_per_unit}</td>
                                <td>{row.total_weight}</td>
                                <td>{row.plength}</td>
                                <td>{row.pbreadth}</td>
                                <td>{row.pheight}</td>
                            </tr>
                        """
        html += "</table>"
        return html

@frappe.whitelist()
def get_box_summary(sales_invoice):
    if frappe.db.exists("Sales Invoice", sales_invoice):
        doc = frappe.get_doc("Sales Invoice", sales_invoice)
        data = frappe.db.sql("""
            SELECT custom_box as box_name, custom_no_of_boxes as total_no,
                   SUM(custom_weight_per_unit_b) as weight_per_unit,
                   SUM(custom_total_weight_of_boxes) as total_weight,
                   SUM(custom_box_length) as blength,
                   SUM(custom_box_height) as bheight,
                   SUM(custom_box_breadth) as bbreadth
            FROM `tabSales Invoice Item`
            WHERE parent = %s
            GROUP BY custom_box
        """, (sales_invoice,), as_dict=True)
        
        data_set = []
        for row in data:
            if row.box_name and row.total_no and row.weight_per_unit and row.total_weight:
                data_set.append({
                    "box": row.box_name,
                    "total_no_of_box": row.total_no,
                    "weight_per_unit": row.weight_per_unit,
                    "total_weight": row.total_weight,
                    "total_length": row.blength,
                    "total_breadth": row.bbreadth,
                    "total_height": row.bheight
                })
        data2 = frappe.db.sql("""
            SELECT custom_pallet as pallet_name, custom_no_of_pallets as total_no, SUM(custom_weight_per_unit_p) as weight_per_unit, SUM(custom_total_weight_of_pallets) as total_weight,
            SUM(custom_pallet_length) as plength,
            SUM(custom_pallet_breadth) as pbreadth,
            SUM(custom_pallet_height) as pheight
            FROM `tabSales Invoice Item`
            WHERE parent = %s
            GROUP BY custom_pallet
        """, (sales_invoice,), as_dict=True)
        data_set2=[]
        for pal in data2:
            if pal.pallet_name and pal.total_no and pal.weight_per_unit and pal.total_weight:
                data_set2.append({
                    "box": pal.pallet_name,
                    "total_no_of_box": pal.total_no,
                    "weight_per_unit": pal.weight_per_unit,
                    "total_weight": pal.total_weight,
                    "total_length": pal.plength,
                    "total_breadth": pal.pbreadth,
                    "total_height": pal.pheight
                })
                 
        return data_set, data_set2

@frappe.whitelist()
def update_workflow(self):
    if self.status == "Draft":
        if self.date_of_shipment and self.shipping_line and self.wonjin_incoterms and (self.customer_incoterms or self.supplier_incoterms) and self.transit_time and self.etd and self.eta:
            if self.cargo_type == "Sea":
                pol = "self.pol_seaport and self.pol_city_seaport and self.pol_country_seaport"
                pod = "self.pod_seaport and self.pod_city_seaport and self.pod_country_seaport"
            elif self.cargo_type == "Air":
                pol = "self.pol_airport and self.pol_city_airport and self.pol_country_airport"
                pod = "self.pod_airport and self.pod_city_airport and self.pod_country_airport"
            else:
                pol = True
                pod = True
            if pol and pod:
                self.status = "Scheduled"
                if self.po_so == "Sales Invoice":
                    frappe.db.set_value("Sales Invoice", self.order_no, "custom_lr_status", "Pending Export")

    if self.status == "Scheduled":
        attachment_count = 0
        row_count = 0
        for row in self.support_documents:
            row_count += 1
            if row.attach:
                attachment_count += 1
        if row_count !=0 and row_count == attachment_count:
            self.status = "Dispatched"
            if self.po_so == "Sales Invoice":
                frappe.db.set_value("Sales Invoice", self.order_no, "custom_lr_status", "Ready to Ship")

    if self.status in ["Dispatched"]:
        if self.boe_number and self.clearance_status and self.appointed_cha_name and self.boe_date and self.payment_challan_attachment and self.payment_date:
            self.status = "In Transit"
            if self.po_so == "Sales Invoice":
                frappe.db.set_value("Sales Invoice", self.order_no, "custom_lr_status", "Shipped")
            
    if self.status in ["In Transit"]:
        if self.attachment and self.date_of_delivery and self.receive_by_name:
            self.status = "Delivered"
    
    if self.status == "Delivered":
        if self.closing_remarks:
            self.status = "Closed"

@frappe.whitelist()
def update_status(name):
    frappe.errprint("hi")
    self = frappe.get_doc("Logistics Request", name)
    if self.status == "Variation - Pending for Finance":
        if self.date_of_shipment and self.shipping_line and self.wonjin_incoterms and (self.customer_incoterms or self.supplier_incoterms) and self.transit_time and self.etd and self.eta:
            if self.cargo_type == "Sea":
                pol = "self.pol_seaport and self.pol_city_seaport and self.pol_country_seaport"
                pod = "self.pod_seaport and self.pod_city_seaport and self.pod_country_seaport"
            elif self.cargo_type == "Air":
                pol = "self.pol_airport and self.pol_city_airport and self.pol_country_airport"
                pod = "self.pod_airport and self.pod_city_airport and self.pod_country_airport"
            else:
                pol = True
                pod = True
            if pol and pod:
                self.status = "Scheduled"
                if self.po_so == "Sales Invoice":
                    frappe.db.set_value("Sales Invoice", self.order_no, "custom_lr_status", "Pending Export")

        attachment_count = 0
        row_count = 0
        for row in self.support_documents:
            row_count += 1
            if row.attach:
                attachment_count += 1
        if row_count !=0 and row_count == attachment_count:
            self.status = "Dispatched"
            if self.po_so == "Sales Invoice":
                frappe.db.set_value("Sales Invoice", self.order_no, "custom_lr_status", "Ready to Ship")

        if self.boe_number and self.clearance_status and self.appointed_cha_name and self.boe_date and self.payment_challan_attachment and self.payment_date:
            self.status = "In Transit"
            if self.po_so == "Sales Invoice":
                frappe.db.set_value("Sales Invoice", self.order_no, "custom_lr_status", "Shipped")
            
        if self.attachment and self.date_of_delivery and self.receive_by_name:
            self.status = "Delivered"
    
        if self.closing_remarks:
            self.status = "Closed"
        self.save(ignore_permissions=True)

@frappe.whitelist()
def get_suplier(name):
    doc = frappe.get_doc("Logistics Request", name)
    sup_list=[]
    for d in doc.ffw_quotation:
        sup_list.append(d.ffw_name)
    return sup_list

@frappe.whitelist()
def validate_ffw_quotation(self):
    if len(self.ffw_quotation) > 0:
        total = 0
        for row in self.product_description:
            total += row.amount
        self.grand_total = total
        for row in self.product_description_so:
            total += row.amount
        self.grand_total = total
        quoted=False
        if self.recommended_ffw:
            for i in self.ffw_quotation:
                if i.ffw_name==self.recommended_ffw:
                    quoted=True
        if quoted==False:
            frappe.throw("Recommended FFW not present in FFW Quotation table")
        if self.quoted_currency=='INR':
            if self.quoted_amount!=self.total_shipment_cost:
                frappe.throw("Total Shipment Cost must be equal to the quoted amount")
        else:
            if self.quoted_value_in_company_currency!=self.total_shipment_cost:
                frappe.throw("Total Shipment Cost must be equal to the quoted amount")