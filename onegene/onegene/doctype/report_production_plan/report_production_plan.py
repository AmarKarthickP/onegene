# Copyright (c) 2025, TEAMPRO and contributors
# For license information, please see license.txt

import frappe
from frappe.model.document import Document
import calendar
from math import ceil
from datetime import date as dt_date


class ReportProductionPlan(Document):

    @frappe.whitelist()
    def get_data(self):
        year = int(self.year)
        month_str = str(self.month).strip().title()

        month_map = {
            'Jan': 1, 'Feb': 2, 'Mar': 3, 'Apr': 4, 'May': 5, 'Jun': 6,
            'Jul': 7, 'Aug': 8, 'Sep': 9, 'Oct': 10, 'Nov': 11, 'Dec': 12
        }

        if month_str.isdigit():
            month = int(month_str)
        else:
            month = month_map.get(month_str)

        if not month or not (1 <= month <= 12):
            frappe.throw(f"Invalid month: {self.month}. Please use Jan, Feb, ... Dec or 1-12.")

        last_day = calendar.monthrange(year, month)[1]

        # Order shifts specifically as 1,2,3,4,5,G
        shift_order = ["1","2","3","4","5","G"]
        shifts = frappe.db.sql("""
            SELECT name 
            FROM `tabShift Type`
        """, as_dict=True)

        # Sort shifts according to custom order
        shifts = sorted(shifts, key=lambda x: shift_order.index(x['name']) if x['name'] in shift_order else 99)
        shift_count = len(shifts)

        # Table header
        data = f'''
        <div style="overflow:auto; width:100%; height:500px; border:1px solid #ccc;">
            <table border="1" cellspacing="0" cellpadding="3" style="border-collapse:collapse; text-align:center; width:100%;">
                <tr>
                    <th rowspan="3" style = background-color:orange>Item Code</th>
                    <th rowspan="3" style = background-color:orange colspan=2>Item Name</th>
                    <th rowspan="3" style = background-color:orange>Item Group</th>
                    <th rowspan="3" nowrap style = background-color:orange>Monthly Schedule</th>
                    <th rowspan="3" style = background-color:orange nowrap>Bin Qty</th>
                    <th rowspan="3" style = background-color:orange nowrap>Per Day Plan</th>
        '''

        # First row: Dates
        for day in range(1, last_day + 1):
            date_str = dt_date(year, month, day).strftime("%b-%d")
            data += f'<th colspan="{shift_count + 1}" style=background-color:orange>{date_str}</th>'
        data += '</tr><tr>'

        # Second row: Plan + Actual
        for _ in range(1, last_day + 1):
            data += f'<td rowspan="2" style="background-color:#A9A9A9; color: white;">Plan</td>'
            data += f'<td colspan="{shift_count}" style="background-color:#808080; color: white;">Actual</td>'
        data += '</tr><tr>'

        # Third row: Shift names
        for _ in range(1, last_day + 1):
            for shift in shifts:
                data += f'<td style="background-color:#E0E0E0">{shift["name"]}</td>'
        data += '</tr>'


        # Conditions
        conditions = {"month": self.month, "year": year}
        customer_condition = ""
        item_group_condition = ""

        if getattr(self, "customer", None):
            customer_condition = "AND customer_name = %(customer)s"
            conditions["customer"] = self.customer

        if getattr(self, "item_group", None):
            item_group_condition = "AND item_group = %(item_group)s"
            conditions["item_group"] = self.item_group

        query = f"""
            SELECT 
                item_code, item_name, item_group, SUM(qty) AS qty
            FROM 
                `tabSales Order Schedule` 
            WHERE 
                docstatus = 1 
                AND schedule_month = %(month)s
                AND schedule_year = %(year)s
                AND item_code in ("A1AC924R64LA1A", "271PL 00A85")
                {customer_condition}
                {item_group_condition}
            GROUP BY item_code
        """

        items = frappe.db.sql(query, conditions, as_dict=True)

        for i in items:
            rej_allowance = frappe.get_value("Item", i.item_code, "rejection_allowance") or 0
            with_rej = round(i.qty * (1 + (rej_allowance / 100)), 2)
            bin_qty = frappe.db.get_value("Item", i.item_code, "pack_size") or 0
            work_days = frappe.db.get_single_value("Production Plan Settings", "working_days") or last_day
            day_plan = ceil(with_rej / int(work_days))

            data += f"""
                <tr>
                    <td nowrap style="text-align:left"> {i.item_code}</td>
                    <td nowrap colspan=2 style="text-align:left">{i.item_name}</td>
                    <td nowrap style="text-align:left">{i.item_group}</td>
                    <td style="text-align:right">{with_rej}</td>
                    <td style="text-align:right">{bin_qty}</td>
                    <td style="text-align:right">{day_plan}</td>
            """

            for day in range(1, last_day + 1):
                current_date = dt_date(year, month, day)

                # Plan quantity for the day
                plan_qty_res = frappe.get_all("Plan Qty",
                    filters={"item": i.item_code, "date": current_date},
                    fields=["sum(qty) as total"]
                )
                plan_qty = plan_qty_res[0].total if plan_qty_res else 0
                data += f"<td>{plan_qty or 0}</td>"

                # Shift-wise quantities
                for shift in shifts:
                    shift_qty_res = frappe.get_all("Quality Inspection",
                        filters={
                            "item_code": i.item_code,
                            "custom_shift": shift["name"],
                            "docstatus": 1,
                            "status": "Accepted",
                            "report_date": current_date
                        },
                        fields=["sum(custom_accepted_qty) as total"]
                    )
                    shift_qty = shift_qty_res[0].total if shift_qty_res else 0
                    data += f"<td>{shift_qty or 0}</td>"

            data += "</tr>"

        data += "</table></div>"
        return data
    
from frappe.utils.csvutils import UnicodeWriter, read_csv_content
from frappe.utils import cstr, add_days, date_diff, getdate
@frappe.whitelist()
def get_template():
    w = UnicodeWriter()
    w = add_header(w)

    frappe.response['result'] = cstr(w.getvalue())
    frappe.response['type'] = 'csv'
    frappe.response['doctype'] = "Plan Qty"

def add_header(w):
    w.writerow(["Item Code","Date", "Plan Qty"])
    return w


@frappe.whitelist()    
def enqueue_create_attendance(attach):
    frappe.enqueue(
        create_plan, # python function or a module path as string
        queue="long", # one of short, default, long
        timeout=80000, # pass timeout manually
        is_async=True, # if this is True, method is run in worker
        now=False, # if this is True, method is run directly (not in a worker) 
        job_name='Plan Qty' ,
        attach = attach
    ) 
    return 'OK'

from frappe.utils.csvutils import UnicodeWriter, read_csv_content
from frappe.utils.file_manager import get_file, upload
@frappe.whitelist()    
def create_plan(attach):
    filepath = get_file(attach)
    pps = read_csv_content(filepath[1])
    for pp in pps:
        if pp[0] != 'Item Code':
            frappe.errprint("Inside Method")
            plan = frappe.new_doc('Plan Qty')
            plan.item = pp[0]
            plan.date = pp[1]
            plan.qty = pp[2]
            plan.save(ignore_permissions=True)
            frappe.db.commit() 

