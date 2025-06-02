import frappe
from frappe import _, msgprint
from frappe.model.naming import make_autoname
from frappe.query_builder import Order
from frappe.query_builder.functions import Sum
from frappe.utils import (
	add_days,
	ceil,
	cint,
	cstr,
	date_diff,
	floor,
	flt,
	formatdate,
	get_first_day,
	get_link_to_form,
	getdate,
	money_in_words,
	rounded,
)
from frappe.utils.background_jobs import enqueue
from datetime import date, datetime, timedelta

import erpnext
from erpnext.accounts.utils import get_fiscal_year
from erpnext.setup.doctype.employee.employee import get_holiday_list_for_employee
from erpnext.utilities.transaction_base import TransactionBase
from hrms.hr.doctype.leave_application.leave_application import LeaveApplication
from hrms.hr.doctype.attendance_request.attendance_request import AttendanceRequest
from hrms.hr.doctype.compensatory_leave_request.compensatory_leave_request import CompensatoryLeaveRequest
from hrms.hr.doctype.leave_application.leave_application import get_approved_leaves_for_period
from hrms.hr.doctype.leave_application.leave_application import get_holidays
# from hrms.hr.doctype.salary_slip.salary_slip import SalarySlip
from frappe.utils import (
	add_days,
	cint,
	cstr,
	date_diff,
	flt,
	formatdate,
	get_fullname,
	get_link_to_form,
	getdate,
	nowdate,
)
from datetime import datetime
from hrms.hr.utils import (
	create_additional_leave_ledger_entry,
	get_holiday_dates_for_employee,
	get_leave_period,
	validate_active_employee,
	validate_dates,
	validate_overlap,
)

from erpnext.buying.doctype.supplier_scorecard.supplier_scorecard import daterange
from erpnext.setup.doctype.employee.employee import get_holiday_list_for_employee
from frappe import throw,_
from frappe.utils import add_days, cint, date_diff, format_date, getdate
from email import message
import frappe
from frappe import _
import datetime, math
from hrms.payroll.doctype.salary_slip.salary_slip import SalarySlip
from hrms.payroll.doctype.salary_slip.salary_slip import get_dates
from hrms.payroll.doctype.salary_slip.salary_slip import check_holiday

class CustomLeaveApplication(LeaveApplication):
	def validate_applicable_after(self):
		# method to restrict the leave type after specific days from joining based on the days from leave type
		if self.leave_type:
			leave_type = frappe.get_doc("Leave Type", self.leave_type)
			if leave_type.applicable_after > 0:
				date_of_joining = frappe.db.get_value("Employee", self.employee, "date_of_joining")
				leave_days = get_approved_leaves_for_period(
					self.employee, False, date_of_joining, self.from_date
				)
				number_of_days = date_diff(getdate(self.from_date), date_of_joining)
				if number_of_days >= 0:
					holidays = 0
					if not frappe.db.get_value("Leave Type", self.leave_type, "include_holiday"):
						holidays = get_holidays(self.employee, date_of_joining, self.from_date)
					number_of_days = number_of_days - leave_days - holidays
					frappe.errprint(number_of_days)
					frappe.errprint(leave_type.applicable_after)
					
					if number_of_days < leave_type.applicable_after:
						frappe.throw(
							_("{0} applicable after {1} working days").format(
								self.leave_type, leave_type.applicable_after
							)
						)
			if leave_type.custom_applicable_before_working_days > 0:
				frappe.errprint("HI")
				date_of_joining = frappe.db.get_value("Employee", self.employee, "date_of_joining")
				leave_days = get_approved_leaves_for_period(
					self.employee, False, date_of_joining, self.from_date
				)
				number_of_days = date_diff(getdate(self.from_date), date_of_joining)
				if number_of_days >= 0:
					frappe.errprint("HIIII")
					holidays = 0
					if not frappe.db.get_value("Leave Type", self.leave_type, "include_holiday"):
						holidays = get_holidays(self.employee, date_of_joining, self.from_date)
					number_of_days = number_of_days - leave_days - holidays
					frappe.errprint(number_of_days)
					frappe.errprint(leave_type.custom_applicable_before_working_days)
					if number_of_days < leave_type.custom_applicable_before_working_days:
						frappe.throw(
							_("{0} applicable after {1} working days").format(
								self.leave_type, leave_type.applicable_after
							)
						)


class CustomCompensatoryLeaveRequest(CompensatoryLeaveRequest):
	def validate(self):
		if self.custom_regular_process_using_holidays_to_coff == 1:
			validate_active_employee(self.employee)
			validate_dates(self, self.work_from_date, self.work_end_date)
			if self.half_day:
				if not self.half_day_date:
					frappe.throw(_("Half Day Date is mandatory"))
				if (
					not getdate(self.work_from_date) <= getdate(self.half_day_date) <= getdate(self.work_end_date)
				):
					frappe.throw(_("Half Day Date should be in between Work From Date and Work End Date"))
			validate_overlap(self, self.work_from_date, self.work_end_date)
			self.validate_holidays()
			self.validate_attendance()
			if not self.leave_type:
				frappe.throw(_("Leave Type is madatory"))
		elif self.custom_coff_claiming_from_normal_days_ot == 1:
			validate_active_employee(self.employee)


	def validate_attendance(self):
		attendance_records = frappe.get_all(
			"Attendance",
			filters={
				"attendance_date": ["between", (self.work_from_date, self.work_end_date)],
				"status": ("in", ["Present", "Work From Home", "Half Day"]),
				"docstatus": 1,
				"employee": self.employee,
			},
			fields=["attendance_date", "status"],
		)

		half_days = [entry.attendance_date for entry in attendance_records if entry.status == "Half Day"]

		if half_days and (not self.half_day or getdate(self.half_day_date) not in half_days):
			frappe.throw(
				_(
					"You were only present for Half Day on {}. Cannot apply for a full day compensatory leave"
				).format(", ".join([frappe.bold(format_date(half_day)) for half_day in half_days]))
			)

		if len(attendance_records) < date_diff(self.work_end_date, self.work_from_date) + 1:
			frappe.throw(_("You are not present all day(s) between compensatory leave request days"))

	def validate_holidays(self):
		holidays = get_holiday_dates_for_employee(self.employee, self.work_from_date, self.work_end_date)
		if len(holidays) < date_diff(self.work_end_date, self.work_from_date) + 1:
			if date_diff(self.work_end_date, self.work_from_date):
				msg = _("The days between {0} to {1} are not valid holidays.").format(
					frappe.bold(format_date(self.work_from_date)), frappe.bold(format_date(self.work_end_date))
				)
			else:
				msg = _("{0} is not a holiday.").format(frappe.bold(format_date(self.work_from_date)))

			frappe.throw(msg)

	def on_submit(self):
		if self.custom_regular_process_using_holidays_to_coff == 1:
			company = frappe.db.get_value("Employee", self.employee, "company")
			date_difference = date_diff(self.work_end_date, self.work_from_date) + 1
			if self.half_day:
				date_difference -= 0.5
			leave_period = get_leave_period(self.work_from_date, self.work_end_date, company)
			if leave_period:
				leave_allocation = self.get_existing_allocation_for_period(leave_period)
				if leave_allocation:
					leave_allocation.new_leaves_allocated += date_difference
					leave_allocation.validate()
					leave_allocation.db_set("new_leaves_allocated", leave_allocation.total_leaves_allocated)
					leave_allocation.db_set("total_leaves_allocated", leave_allocation.total_leaves_allocated)

					# generate additional ledger entry for the new compensatory leaves off
					create_additional_leave_ledger_entry(
						leave_allocation, date_difference, add_days(self.work_end_date, 1)
					)

				else:
					leave_allocation = self.create_leave_allocation(leave_period, date_difference)
				self.db_set("leave_allocation", leave_allocation.name)
			else:
				frappe.throw(
					_("There is no leave period in between {0} and {1}").format(
						format_date(self.work_from_date), format_date(self.work_end_date)
					)
				)
		else:
			company = frappe.db.get_value("Employee", self.employee, "company")
			date_difference = self.custom_no_of_coff_taken_days
			if self.custom_available_coff_days < self.custom_no_of_coff_taken_days:
				frappe.throw(
					_("Leave taken days count is greater the available days"
					)
				)
			else:
				leave_period = get_leave_period(self.work_from_date, self.work_end_date, company)
				if leave_period:
					leave_allocation = self.get_existing_allocation_for_period(leave_period)
					if leave_allocation:
						frappe.errprint(leave_allocation.name)
						leave_allocation.new_leaves_allocated += date_difference
						leave_allocation.validate()
						leave_allocation.db_set("new_leaves_allocated", leave_allocation.total_leaves_allocated)
						leave_allocation.db_set("total_leaves_allocated", leave_allocation.total_leaves_allocated)

						# generate additional ledger entry for the new compensatory leaves off
						create_additional_leave_ledger_entry(
							leave_allocation, date_difference, add_days(self.work_end_date, 1)
						)

					else:
						leave_allocation = self.create_leave_allocation(leave_period, date_difference)
					self.db_set("leave_allocation", leave_allocation.name)


	def on_cancel(self):
		if self.custom_regular_process_using_holidays_to_coff == 1:
			if self.leave_allocation:
				date_difference = date_diff(self.work_end_date, self.work_from_date) + 1
				if self.half_day:
					date_difference -= 0.5
				leave_allocation = frappe.get_doc("Leave Allocation", self.leave_allocation)
				if leave_allocation:
					leave_allocation.new_leaves_allocated -= date_difference
					if leave_allocation.new_leaves_allocated - date_difference <= 0:
						leave_allocation.new_leaves_allocated = 0
					leave_allocation.validate()
					leave_allocation.db_set("new_leaves_allocated", leave_allocation.total_leaves_allocated)
					leave_allocation.db_set("total_leaves_allocated", leave_allocation.total_leaves_allocated)

					# create reverse entry on cancelation
					create_additional_leave_ledger_entry(
						leave_allocation, date_difference * -1, add_days(self.work_end_date, 1)
					)
		else:
			if self.leave_allocation:
				date_difference = self.custom_no_of_coff_taken_days
				leave_allocation = frappe.get_doc("Leave Allocation", self.leave_allocation)
				if leave_allocation:
					leave_allocation.new_leaves_allocated -= date_difference
					if leave_allocation.new_leaves_allocated - date_difference <= 0:
						leave_allocation.new_leaves_allocated = 0
					leave_allocation.validate()
					leave_allocation.db_set("new_leaves_allocated", leave_allocation.total_leaves_allocated)
					leave_allocation.db_set("total_leaves_allocated", leave_allocation.total_leaves_allocated)

					# create reverse entry on cancelation
					create_additional_leave_ledger_entry(
						leave_allocation, date_difference * -1, add_days(self.work_end_date, 1)
					)


	def get_existing_allocation_for_period(self, leave_period):
		leave_allocation = frappe.db.sql(
			"""
			select name
			from `tabLeave Allocation`
			where employee=%(employee)s and leave_type=%(leave_type)s
				and docstatus=1
				and (from_date between %(from_date)s and %(to_date)s
					or to_date between %(from_date)s and %(to_date)s
					or (from_date < %(from_date)s and to_date > %(to_date)s))
		""",
			{
				"from_date": leave_period[0].from_date,
				"to_date": leave_period[0].to_date,
				"employee": self.employee,
				"leave_type": self.leave_type,
			},
			as_dict=1,
		)

		if leave_allocation:
			return frappe.get_doc("Leave Allocation", leave_allocation[0].name)
		else:
			return False

	def create_leave_allocation(self, leave_period, date_difference):
		is_carry_forward = frappe.db.get_value("Leave Type", self.leave_type, "is_carry_forward")
		allocation = frappe.get_doc(
			dict(
				doctype="Leave Allocation",
				employee=self.employee,
				employee_name=self.employee_name,
				leave_type=self.leave_type,
				from_date=add_days(self.work_end_date, 1),
				to_date=leave_period[0].to_date,
				carry_forward=cint(is_carry_forward),
				new_leaves_allocated=date_difference,
				total_leaves_allocated=date_difference,
				description=self.reason,
			)
		)
		allocation.insert(ignore_permissions=True)
		allocation.submit()
		return allocation
	
class CustomSalarySlip(SalarySlip):	
	import datetime
	def get_date_details(self):
		# attendance = frappe.db.sql("""SELECT * FROM `tabAttendance` WHERE attendance_date BETWEEN '%s' AND '%s' AND employee = '%s' AND docstatus != 2 AND status = 'Absent'""" % (self.start_date, self.end_date, self.employee), as_dict=True)
		# absenteeism_penalty = 0
		# # frappe.errprint(attendance)
		# if attendance:
		# 	for att in attendance:
		# 		if(att.custom_employee_category not in ["Staff,Sub Staff"]):
		# 			# frappe.errprint(att.employee)
		# 			absenteeism_penalty +=1
		# # frappe.errprint(absenteeism_penalty)
		# self.custom_absenteeism_penalty_days_ = absenteeism_penalty
		# check for unclaimed ot hours from ot balance document
		if frappe.db.exists("OT Balance",{'from_date':self.start_date,'to_date':self.end_date,'employee':self.employee}):
			ot_bal=frappe.db.get_value("OT Balance",{'from_date':self.start_date,'to_date':self.end_date,'employee':self.employee},['ot_balance'])
			self.custom_overtime_hours=ot_bal
		# take count of canteen check in to deduct from the salary
		days = frappe.db.count("Employee Checkin", {
		'employee': self.employee,
		'custom_checkin_date': ["between", (self.start_date, self.end_date)],
		'device_id': "Canteen"
		}) or 0
		self.custom_canteen_deduction_days = days
		# calculation for shift allowance
		if self.custom_employee_category in ("Staff", "Operator"):
			attendance_count = frappe.db.count("Attendance", filters={
				"attendance_date": ["between", (self.start_date, self.end_date)],
				"employee": self.employee,
				"shift": ['in', ("3", "5")],
				"docstatus": 1,
			})
			self.custom_shift_allowance_days = attendance_count
		else:
			self.custom_shift_allowance_days = 0
		if self.custom_employee_category == "Apprentice":
			if self.custom_date_of_joining == "":
				doj = frappe.get_value("Employee", {'name': self.employee}, ['date_of_joining'])
			else:
				doj = self.custom_date_of_joining
			if datetime.datetime.strptime(str(self.start_date), '%Y-%m-%d').date() < datetime.datetime.strptime(str(doj), '%Y-%m-%d').date():
				start_date = self.custom_date_of_joining
			else:
				start_date = self.start_date

			relv_date = frappe.db.get_value('Employee', self.employee, 'relieving_date')
			if relv_date:
				end_date_obj = getdate(self.end_date)
				if relv_date <= end_date_obj:
					end_date = relv_date
				else:
					end_date = self.end_date
			else:
				end_date = self.end_date
			dates = get_dates(start_date,end_date)
			absent_days = 0
			for date in dates:
				hh = check_holiday(date,self.employee)
				if not hh :
					if frappe.db.exists("Attendance",{'employee':self.employee,'attendance_date':date,'docstatus':1,'status':"Absent"}):
						absent_days += 1
			# if absent_days > 0 :
			# 	self.payment_days = self.payment_days - absent_days

class CustomAttendanceRequest(AttendanceRequest):
	# attendance only updated only application got approved
	def on_submit(self):
		if self.workflow_state=='Approved':
			frappe.errprint("test")
			self.create_attendance_records()
		else:
			pass

