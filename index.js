/* eslint-disable space-before-function-paren */
/**
 * Copyright 2023 Chris Colohan.  All Rights Reserved.
 *
 * Based on Firebase sample code from Google that bears this copyright:
 *
 * Copyright 2015 Google Inc. All Rights Reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
'use strict';

import * as functions from 'firebase-functions';
import admin from 'firebase-admin';
import sanitizeHtml from 'sanitize-html';

admin.initializeApp();
const db = admin.firestore();
const bucket = admin.storage().bucket();

/**
 * Determines if value is a correctly-formatted dollar amount.
 * @param {string} value The string value to be checked.
 * @return {boolean} True if formatted correctly.
 */
function isValidDollarAmount(value) {
  // Why 20?  Just an arbitrary but huge number:
  return typeof value === 'string' && value.length < 20 && /^[0-9]*(\.[0-9][0-9])?$/.test(value);
}

const MAX_STRING_LENGTH = 16384;

export const submitExpense = functions
  //  .runWith({
  //    enforceAppCheck: true // Requests without valid App Check tokens will be rejected.
  //  })
  .https.onCall(async (data, context) => {
    if (!context.auth) {
      // Throwing an HttpsError so that the client gets the error details.
      throw new functions.https
        .HttpsError('failed-precondition',
          'The function must only be called while authenticated.');
    }
    const userName = context.auth.token.email;
    const school = data.school;
    functions.logger.info(`submitExpense request received (${school}: ${userName}):`, { data, context });

    const purpose = data.purpose;
    const approverId = data.approver_id;
    const name = data.name;
    const addressStreet = data.address_street;
    const addressCity = data.address_city;
    const addressState = data.address_state;
    const addressZip = data.address_zip;
    const email = data.email;
    const phone = data.phone;
    const payVia = data.pay_via;
    const rememberPayee = data.remember_payee;
    const advanceReceived = data.advance_received;
    const donate = data.donate;
    const receipts = data.receipts;
    const notes = data.notes;

    return await submitExpenseImpl(userName, school, purpose, approverId, name,
      addressStreet, addressCity, addressState, addressZip, email, phone,
      payVia, rememberPayee, advanceReceived, donate, receipts, notes);
  });

async function submitExpenseImpl(userName, school, purpose, approverId, name,
  addressStreet, addressCity, addressState, addressZip, email, phone,
  payVia, rememberPayee, advanceReceived, donate, receipts, notes) {
  const trace = traceStart();
  try {
    function checkString(arg, nonzeroLen) {
      if (typeof arg !== 'string') {
        throw new functions.https
          .HttpsError('invalid-argument',
            'Type error.');
      }
      if (arg.length > MAX_STRING_LENGTH) {
        throw new functions.https
          .HttpsError('invalid-argument',
            'Argument too long.');
      }
      if (nonzeroLen && arg.length === 0) {
        throw new functions.https
          .HttpsError('invalid-argument',
            'Missing required argument.');
      }
    }
    checkString(school, true);
    checkString(purpose, true);
    checkString(approverId, true);
    checkString(name, true);
    checkString(addressStreet, false);
    checkString(addressCity, false);
    checkString(addressState, false);
    checkString(addressZip, false);
    checkString(email, false);
    checkString(phone, false);
    checkString(payVia, false);
    checkString(notes, false);

    if (!Array.isArray(receipts)) {
      throw new functions.https
        .HttpsError('invalid-argument',
          'Type error.');
    }
    if (!isValidDollarAmount(advanceReceived)) {
      throw new functions.https
        .HttpsError('invalid-argument',
          'Advance must be a dollar amount.');
    }
    if (!isValidDollarAmount(donate)) {
      throw new functions.https
        .HttpsError('invalid-argument',
          'Donation must be a dollar amount.');
    }
    for (const receipt of receipts) {
      checkString(receipt.name, true);
      checkString(receipt.description, true);
      checkString(receipt.path, true);
      if (!isValidDollarAmount(receipt.amount)) {
        throw new functions.https
          .HttpsError('invalid-argument',
            'Receipt amount must be a dollar amount.');
      }
      // If the user sends us a corrupted file path, do we care to do anything
      // about it?
    }

    try {
      const newExpenseNum = await db.runTransaction(async (transaction) => {
        trace.trace('runTransaction');
        let schoolUserDoc = getSchoolUser(school, userName, transaction);
        let schoolDoc = getSchool(school, transaction);
        schoolUserDoc = await schoolUserDoc;
        schoolDoc = await schoolDoc;
        trace.trace('schoolUser and School');
        if (!schoolDoc) {
          throw new Error('School does not exist!');
        }
        const schoolData = schoolDoc.data();

        if (approverId < 0 || approverId >= schoolData.approvers.length) {
          throw new Error('Invalid budget specified.');
        }
        const approvers = schoolData.approvers[approverId].ids;
        const budget = schoolData.approvers[approverId].role;

        if (payVia.length === 0) {
          // FIXME: what do we do if the Pay Via list is undefined?
          payVia = schoolData.pay_via.length > 0 ? schoolData.pay_via[0].type : 'Check';
        }

        // -------------------------------------------------------------------
        // Writes start here, no more database reads below this line
        const newExpenseNum = schoolData.last_expense_num + 1;
        const schoolRef = db.collection('schools').doc(school);
        await transaction.update(schoolRef, { last_expense_num: newExpenseNum });
        trace.trace('updateExpenseNum');

        // Ensure this user exists in this school (may not if sysadmin, or due to
        // other bug/race):
        if (!schoolUserDoc) {
          createSchoolUser(school, userName, transaction);
          trace.trace('addUser');
        }

        const payee = {
          name,
          address_street: addressStreet,
          address_city: addressCity,
          address_state: addressState,
          address_zip: addressZip,
          email,
          phone
        };

        let notesArray = [];
        if (notes.length > 0) {
          notesArray = [{
            user: userName,
            note: notes,
            date: Date.now()
          }];
        }

        const expenseRef = db.collection('schools').doc(school)
          .collection('expenses').doc(newExpenseNum.toString());
        await transaction.set(expenseRef, {
          expense_num: newExpenseNum,
          submitter: userName,
          date: Date.now(),
          state: 'submitted',
          purpose,
          budget,
          approvers,
          payee,
          pay_via: payVia,
          advance_received: advanceReceived,
          donate,
          receipts,
          notes: notesArray,
          last_email_sent: 0,
          version: 0
        });
        trace.trace('set');

        if (rememberPayee) {
          const userRef = db.collection('users').doc(userName);
          await transaction.update(userRef, { saved_payee: payee });
          trace.trace('updateSavedPayee');
        }

        // Need to return this from the transaction instead of modifying a local
        // variable because transactions can't change application state:
        return newExpenseNum;
      });

      return { expense_num: newExpenseNum };
    } catch (error) {
      functions.logger.error('Unable to write expense to database: ', { error });
      throw new functions.https
        .HttpsError('internal',
          'Unable to write expense to database.');
    }
  } finally {
    trace.end();
  }
}

// TODO: create a function to track every file upload

async function checkIfSchoolValid(school, transaction) {
  if (school.length === 0) {
    return false;
  }
  const schoolRef = db.collection('schools').doc(school);
  return transaction
    ? (await transaction.get(schoolRef)).exists
    : (await schoolRef.get()).exists;
}

async function getUser(email, transaction) {
  try {
    const userRef = db.collection('users').doc(email);
    const userRecord = transaction
      ? await transaction.get(userRef)
      : await userRef.get();
    return (userRecord && userRecord.exists) ? userRecord : null;
  } catch (error) {
    functions.logger.warn(`Unable to look up user '${email}'`, { error });
    return null;
  }
}

async function getSchool(school, transaction) {
  try {
    const schoolRef = db.collection('schools').doc(school);
    const schoolRecord = transaction
      ? await transaction.get(schoolRef)
      : await schoolRef.get();
    return schoolRecord.exists ? schoolRecord : null;
  } catch (error) {
    functions.logger.warn(`Unable to look up school '${school}'`, { error });
    return null;
  }
}

async function getSchoolUser(school, email, transaction) {
  try {
    const schoolUserRef = db.collection('schools').doc(school).collection('school_users').doc(email);
    const schoolUserRecord = transaction
      ? await transaction.get(schoolUserRef)
      : await schoolUserRef.get();
    return schoolUserRecord.exists ? schoolUserRecord : null;
  } catch (error) {
    functions.logger.warn(`Unable to look up school user '${school}/${email}'`, { error });
    return null;
  }
}

async function createSchoolUser(school, email, transaction) {
  try {
    const schoolUserRef = db.collection('schools').doc(school).collection('school_users').doc(email);
    await transaction.set(schoolUserRef, {
      email,
      is_school_admin: false,
      is_check_writer: false,
      is_auditor: false
    });
  } catch (error) {
    functions.logger.warn(`Unable to create school user '${school}/${email}'`, { error });
    throw new functions.https
      .HttpsError('internal',
        'Unable to create school user.');
  }
}

async function getExpense(school, expense, transaction) {
  try {
    const expenseRef = db.collection('schools').doc(school).collection('expenses').doc(expense);
    const expenseRecord = transaction
      ? await transaction.get(expenseRef)
      : await expenseRef.get();
    return expenseRecord.exists ? expenseRecord : null;
  } catch (error) {
    functions.logger.warn(`Unable to look up expense '${school}/${expense}'`, { error });
    return null;
  }
}

export const getUserData = functions
  //  .runWith({
  //    enforceAppCheck: true // Requests without valid App Check tokens will be rejected.
  //  })
  .https.onCall(async (data, context) => {
    if (!context.auth) {
      // Throwing an HttpsError so that the client gets the error details.
      throw new functions.https
        .HttpsError('failed-precondition',
          'The function must only be called while authenticated.');
    }

    const uid = context.auth.uid;
    const userName = context.auth.token.email;
    const name = context.auth.token.name;
    const schoolURL = data.school;
    functions.logger.info(`getUserData request received (${schoolURL}: ${userName}):`, { data, context });

    return await getUserDataImpl(uid, userName, name, schoolURL);
  });

async function getUserDataImpl(uid, userName, name, schoolURL) {
  const trace = traceStart();
  try {
    // When we have a user log in, we need to handle all the cases:

    // 1. We know about this user already.  This is the simple case, simply return
    //    their data.  If the school URL is valid and is new to them, add them to
    //    the specified school.

    // 2. We don't know about this user, and they don't come from a valid school
    //    URL.  Just return an error, we can't help them.

    // 3. We don't know about this user, but they *do* come from a valid school
    //    URL.  Create a new user for them, and assign them to this school.

    let user;
    let schoolURLisValid;

    try {
      const schoolURLisValidPromise = checkIfSchoolValid(schoolURL);

      // Look up user in firestore:
      const userResult = await getUser(userName);
      trace.trace('getUser');

      schoolURLisValid = await schoolURLisValidPromise;
      trace.trace('checkIfSchoolValid');

      if (!schoolURLisValid && schoolURL === 'demo') {
        // Special case:  the user has asked for a demo, but the demo school
        // doesn't exist yet.  Create it:
        addSchoolImpl('demo', 'Demo Elementary');
        trace.trace('addSchoolImpl');
        schoolURLisValid = true;
      }

      if (userResult) {
        user = userResult.data();
      } else {
        if (!schoolURLisValid) {
          throw new functions.https
            .HttpsError('not-found',
              'User not found: ' + userName);
        }
        user = {
          email: userName
        };
      }

      let updateUser = false;
      if (user.id === undefined) {
        user.id = uid;
        updateUser = true;
      }
      if (user.name === undefined) {
        user.name = name;
        updateUser = true;
      }
      if (user.is_sysadmin === undefined) {
        user.is_sysadmin = false;
        updateUser = true;
      }
      if (updateUser) {
        await db.collection('users').doc(userName).set(user);
        trace.trace('updateUser');
      }
    } catch (error) {
      throw new functions.https
        .HttpsError('not-found',
          'Error looking up user: ' + userName);
    }

    const schools = [];
    try {
      let schoolURLfound = false;
      const schoolRequests = [];

      if (user.is_sysadmin && schoolURL !== 'demo') {
        // If the user is a global sysadmin, then just give them all the schools
        // with all the powers:
        const schoolList = await db.collection('schools').get();
        trace.trace('schoolList');

        const schoolData = [];
        schoolList.forEach((school) => {
          if (school.data().id !== 'demo') {
            schoolData.push({ school, school_user: getSchoolUser(school.data().id, userName) });
          }
        });

        for (const schoolRec of schoolData) {
          const school = schoolRec.school;
          const schoolUser = await schoolRec.school_user;

          let isApprover = false;
          for (const approver of school.data().approvers) {
            if (approver.ids.includes(userName)) {
              isApprover = true;
            }
          }
          schools.push({
            is_school_admin: schoolUser ? schoolUser.data().is_school_admin : false,
            is_check_writer: schoolUser ? schoolUser.data().is_check_writer : false,
            is_auditor: schoolUser ? schoolUser.data().is_auditor : false,
            is_approver: isApprover,
            name: school.data().name,
            id: school.data().id,
            approvers: school.data().approvers,
            pay_via: school.data().pay_via
          });
        }
        trace.trace('get schoolData');
      } else {
        // Now need to know what schools have this user:
        const schoolsQuery = db.collectionGroup('school_users')
          .where('email', '==', userName);
        const schoolsResult = await schoolsQuery.get();
        trace.trace('schoolsQuery');

        if (schoolsResult.size === 0) {
          functions.logger.info('No schools!');
        }

        schoolsResult.forEach((schoolUser) => {
          const schoolRef = schoolUser.ref.parent.parent;
          const school = schoolRef.id;
          if (school === 'demo' && schoolURL !== 'demo') {
            // Do not include the demo school in our results unless that school is explicitly asked for.
          } else {
            // In demo mode you are always the admin, nobody can take that away from you.
            schools.push({
              id: school,
              is_school_admin: school === 'demo' ? true : schoolUser.data().is_school_admin,
              is_check_writer: schoolUser.data().is_check_writer,
              is_auditor: schoolUser.data().is_auditor
            });
            schoolRequests.push(schoolRef.get());
            if (school === schoolURL) {
              schoolURLfound = true;
            }
          }
        });
        trace.trace('schoolUsers');

        // If we've gotten this far and the user is not a user of the school in
        // their URL, add them:
        if (!schoolURLfound && schoolURLisValid) {
          await db.collection('schools').doc(schoolURL).collection('school_users')
            .doc(userName).set({
              email: userName,
              is_school_admin: false,
              is_check_writer: false,
              is_auditor: false
            });
          trace.trace('setSchoolUser');
          schools.push({
            id: schoolURL,
            is_school_admin: schoolURL === 'demo',
            is_check_writer: false,
            is_auditor: false
          });
          const schoolURLRef = db.collection('schools').doc(schoolURL);
          schoolRequests.push(schoolURLRef.get());
        }

        for (let i = 0; i < schools.length; i++) {
          const school = (await schoolRequests[i]).data();
          schools[i].name = school.name;
          schools[i].id = school.id;
          schools[i].approvers = school.approvers;
          schools[i].pay_via = school.pay_via;

          schools[i].is_approver = false;
          // Client doesn't need email addresses, so don't give it to them:
          for (const approver of schools[i].approvers) {
            if (approver.ids.includes(userName)) {
              schools[i].is_approver = true;
            }
            delete approver.ids;
          }

          if (school.id === schoolURL) {
            schoolURLfound = true;
          }

          // console.log('ID of school:   ', schools[i].id);
          // console.log('Is school admin:', schools[i].is_school_admin);
          // console.log('Is check writer:', schools[i].is_check_writer);
          // console.log('Is auditor:     ', schools[i].is_auditor);
          // console.log('Name of school: ', schools[i].name);
          // console.log('ID of school:   ', schools[i].id);
          // console.log('Approvers:      ', schools[i].approvers);
          // console.log('Pay_via:        ', schools[i].pay_via);
        }
        trace.trace('schoolRequests');
      }
    } catch (error) {
      throw new functions.https
        .HttpsError('not-found',
          'Error finding schools for user: ' + userName);
    }

    // console.log(`schools: ${schools}`);
    // console.log(`saved_payee: ${user.saved_payee}`);
    // console.log(`is_sysadmin: ${user.is_sysadmin}`);
    return {
      schools,
      saved_payee: user.saved_payee,
      is_sysadmin: user.is_sysadmin
    };
  } finally {
    trace.end();
  }
}

// async function createUser(schoolId, userId, isCheckWriter, isSchoolAdmin, isAuditor) {
//   await db.collection('users').doc(userId).set({
//     email: userId
//   }, { merge: true });

//   const schoolUser = {
//     email: userId,
//     is_check_writer: isCheckWriter,
//     is_school_admin: isSchoolAdmin,
//     is_auditor: isAuditor
//   };

//   await db.collection('schools').doc(schoolId).collection('school_users').doc(userId)
//     .set(schoolUser, { merge: true });
// }

async function emailToName(email, transaction) {
  const nameResponse = await getUser(email, transaction);
  if (nameResponse && nameResponse.data() && nameResponse.data().name) {
    return nameResponse.data().name;
  } else {
    return '';
  }
}

async function schoolToName(school, transaction) {
  return (await getSchool(school, transaction)).data().name;
}

async function getCheckWriters(school, transaction) {
  const checkWriters = [];
  const checkWritersQuery = db.collection('schools').doc(school)
    .collection('school_users').where('is_check_writer', '==', true);
  const checkWritersQueryResult = await transaction.get(checkWritersQuery);
  checkWritersQueryResult.forEach((schoolUser) => {
    checkWriters.push(schoolUser.id);
  });
  return checkWriters;
}

async function sendEmail(expenseRef, expense, school, isReminder) {
  functions.logger.info(`sendEmail (${school} expense_num=${expense.expense_num} isReminder=${isReminder})`, { expense });

  // functions.logger.info(`sendEmail DEBUG runTransaction (subject=${subject} text=${text})`, { html });
  try {
    await db.runTransaction(async (transaction) => {
      let sum = 0;
      for (const receipt of expense.receipts) {
        sum += parseFloat(receipt.amount);
      }
      sum -= expense.advance_received;
      sum -= expense.donate;

      const submitterName = await emailToName(expense.submitter, transaction);
      const checkWriters = await getCheckWriters(school, transaction);
      // Potential issue here:  this is in the timezone of the server, and not of the recipient.
      const submittedDate = (new Date(expense.date)).toLocaleDateString();

      let recipients = [];
      switch (expense.state) {
        case 'submitted':
          recipients = expense.approvers;
          break;
        case 'approved':
          recipients = checkWriters;
          break;
        case 'paid':
          recipients = [expense.submitter];
          break;
        case 'rejected':
          recipients = [expense.submitter];
          break;
        default:
          functions.logger.error('Unknown expense state: ' + expense.state);
          break;
      }

      function listsHaveElementsInCommon(list1, list2) {
        for (const e of list1) {
          if (list2.includes(e)) {
            return true;
          }
        }
        return false;
      }
      const approvedByBudgetOwner = expense.approved_by
        ? listsHaveElementsInCommon(expense.approvers, expense.approved_by)
        : false;

      const emailsToSend = new Map();
      switch (expense.state) {
        case 'submitted':
        case 'approved':
          for (const recipient of recipients) {
            // FIXME:  these parameters don't work with approversNeeded:
            const approvalCount = await approversNeeded((await getSchool(school, transaction)).data(),
              expense, recipient, transaction);
            if (checkWriters.includes(recipient) && approvalCount === 0) {
              const email = emailsToSend.has('PAY') ? emailsToSend.get('PAY') : { recipients: [] };
              email.recipients.push(recipient);
              email.header = 'PAY';
              if (approvedByBudgetOwner) {
                email.nextstep = 'This expense has received all needed approvals.  You can pay it.';
                email.action = 'Please click here to pay:';
                emailsToSend.set('PAY1', email);
              } else if (expense.approvers.includes(recipient)) {
                email.nextstep = 'This expense needs to be approved.  ' +
                  'Since you are both an approver and check writer you can now pay it.';
                email.action = 'Please click here to approve and pay:';
                emailsToSend.set('PAY2', email);
              } else {
                email.nextstep = 'This expense has *not been approved* by a budget owner, ' +
                  'but a check writer has approved it.  You can now pay it.';
                email.action = 'Please click here to check approval status and pay:';
                emailsToSend.set('PAY3', email);
              }
            } else {
              if (expense.approved_by && expense.approved_by.includes(recipient)) {
                // Do not send an email to this person, they've already approved
                // the expense and do not (yet) have the power to pay it.
              } else {
                const email = emailsToSend.has('APPROVE') ? emailsToSend.get('APPROVE') : { recipients: [] };
                email.recipients.push(recipient);
                email.header = 'APPROVE';
                if (approvedByBudgetOwner) {
                  email.nextstep = 'This expense has been approved by a budget owner.  ' +
                    `It needs to be approved by you and ${approvalCount} other check writers to be paid.`;
                  email.action = 'Please click here to approve:';
                  emailsToSend.set('APPROVE1', email);
                } else {
                  email.nextstep = 'This expense needs to be approved by a budget owner.';
                  email.action = 'Please click here to approve:';
                  emailsToSend.set('APPROVE2', email);
                }
              }
            }
          }
          break;
        case 'paid':
          emailsToSend.set('PAID', {
            recipients,
            header: 'PAID',
            nextstep: 'This expense has been marked paid.  Expect payment soon.'
          });
          break;
        case 'rejected':
          emailsToSend.set('REJECTED', {
            recipients,
            header: 'REJECTED',
            nextstep: 'This expense has been marked rejected.'
          });
          break;
      }

      function notBlank(value, text) {
        if (value) {
          return text;
        } else {
          return '';
        }
      }
      // functions.logger.info(`sendEmail DEBUG (name=${name} schoolName=${schoolName})`);

      async function genEmailBody(schoolName, expense, submitterName, nextStep, action, expenseURL) {
        function sanitize(dirty) {
          return sanitizeHtml(dirty, {
            allowedTags: [],
            allowedAttributes: {},
            disallowedTagsMode: 'recursiveEscape'
          });
        }
        let notesText = '';
        let notesHTML = '';
        for (const note of expense.notes) {
          notesText += '===\n' +
            `${await emailToName(note.user, transaction)} <${note.user}> [${(new Date(note.date)).toLocaleString()}]\n` +
            '\n' +
            `${note.note}\n`;
          notesHTML += '<tr><td>\n' +
            `<p>${sanitize(await emailToName(note.user, transaction))} &lt;${sanitize(note.user)}&gt; [${(new Date(note.date)).toLocaleString()}]\n` +
            `<p>${sanitize(note.note)}\n` +
            '</td></tr>\n';
        }
        const formatter = new Intl.NumberFormat('en-US', {
          style: 'currency',
          currency: 'USD'
        });
        const text =
          `${nextStep}\n` +
          '\n' +
          `School:          ${schoolName}\n` +
          `Expense Number:  ${expense.expense_num}\n` +
          `Submitted By:    ${submitterName} <${expense.submitter}>\n` +
          `Date Submitted:  ${submittedDate}\n` +
          `Purpose:         ${expense.purpose}\n` +
          `Budget:          ${expense.budget}\n` +
          notBlank(expense.pay_via, `Pay Via:         ${expense.pay_via}\n`) +
          notBlank(expense.payee.name, `Pay To:          ${expense.payee.name}\n`) +
          notBlank(expense.payee.address_street, `                 ${expense.payee.address_street}\n`) +
          ((expense.payee.address_city || expense.payee.address_state) ? '                 ' : '') +
          ((expense.payee.address_city) ? `${expense.payee.address_city}` : '') +
          ((expense.payee.address_city && expense.payee.address_state) ? ', ' : '') +
          ((expense.payee.address_state) ? `${expense.payee.address_state}\n` : '\n') +
          notBlank(expense.payee.address_zip, `                 ${expense.payee.address_zip}\n`) +
          notBlank(expense.payee.email, `                 ${expense.payee.email}\n`) +
          notBlank(expense.payee.phone, `                 ${expense.payee.phone}\n`) +
          `Total requested: ${formatter.format(sum)}\n` +
          '\n' +
          `${action}\n` +
          `${expenseURL}\n` +
          '\n' +
          'Notes:\n' +
          `${notesText}`;

        const html = '<!DOCTYPE html><html lang="en"><head>' +
          '<div style="padding:10px;font-size:20px;color:#eceff1;font-family:\'Roboto\',\'Helvetica\',sans-serif;background-color:#263238">' +
          `<a style="text-decoration:none;color:#eceff1;margin:auto;display:flex;align-items:center" href="${expenseURL}">` +
          '<img src="https://www.parentpayback.com/images/logo.png" alt="Parent Payback Logo" width="40">' +
          '&nbsp;&nbsp;Parent Payback' +
          '</a></div></head><body>\n' +
          `<p>${nextStep}</p>\n` +
          '<table>\n' +
          `<tr><td>School:</td><td>${schoolName}</td></tr>\n` +
          `<tr><td>Expense Number:</td><td>${expense.expense_num}</td></tr>\n` +
          `<tr><td>Date Submitted:</td><td>${submittedDate}</td></tr>\n` +
          `<tr><td>Submitted By:</td><td>${submitterName} &lt;${expense.submitter}&gt;</td></tr>\n` +
          `<tr><td>Purpose:</td><td>${expense.purpose}</td></tr>\n` +
          `<tr><td>Budget:</td><td>${expense.budget}</td></tr>\n` +
          notBlank(expense.pay_via, `<tr><td>Pay Via:</td><td>${expense.pay_via}</td></tr>\n`) +
          notBlank(expense.payee.name, `<tr><td>Pay To:</td><td>${expense.payee.name}</td></tr>\n`) +
          notBlank(expense.payee.address_street, `<tr><td></td><td>${expense.payee.address_street}</td></tr>`) +
          ((expense.payee.address_city || expense.payee.address_state) ? '<tr><td></td><td>' : '') +
          ((expense.payee.address_city) ? `${expense.payee.address_city} ` : '') +
          ((expense.payee.address_city && expense.payee.address_state) ? ', ' : '') +
          ((expense.payee.address_state) ? `${expense.payee.address_state} ` : '') +
          ((expense.payee.address_city || expense.payee.address_state) ? '</td></tr>\n' : '') +
          notBlank(expense.payee.address_zip, `<tr><td></td><td>${expense.payee.address_zip}</td></tr>\n`) +
          notBlank(expense.payee.email, `<tr><td></td><td>${expense.payee.email}</td></tr>\n`) +
          notBlank(expense.payee.phone, `<tr><td></td><td>${expense.payee.phone}</td></tr>\n`) +
          `<tr><td>Total requested:</td><td>${formatter.format(sum)}</td></tr>\n` +
          '</table>\n' +
          `<p>${action}\n` +
          `<a href="${expenseURL}"> ${expenseURL}</a>\n` +
          '<p>Notes:\n' +
          '<table>\n' +
          `${notesHTML}\n` +
          '</table>\n' +
          '</body></html>';
        return [text, html];
      }

      const schoolName = await schoolToName(school, transaction);
      const expenseURL = `https://parentpayback.com/${school}?expense=${expense.expense_num}`;

      for (const [, email] of emailsToSend) {
        const [text, html] = await genEmailBody(schoolName, expense, submitterName,
          email.nextstep, email.action, expenseURL);
        const subject = `${(isReminder ? 'Reminder: ' : '')}${email.header}: ${submitterName} on ` +
          `${submittedDate}`;
        const emailRef = db.collection('outbound_email').doc();
        // functions.logger.info('sendEmail DEBUG emailRef', { emailRef });
        await transaction.set(emailRef, {
          to: email.recipients,
          bcc: 'emaillog@parentpayback.com',
          message: {
            subject,
            text,
            html
          }
        });
      }
      // console.log('Email body:\n', text);

      const time = Date.now();
      await transaction.update(expenseRef, { last_email_sent: time });
    });
  } catch (error) {
    functions.logger.error('Unable to send email: ', { error });
    throw new functions.https
      .HttpsError('internal',
        'Unable to send email:' + error);
  }
}

export const sendEmailPeriodic = functions.pubsub.schedule('every day 09:00').onRun(async (context) => {
  functions.logger.info('Daily email run');
  const now = Date.now();
  const ONE_WEEK = 1000 * 60 * 60 * 24 * 7;
  const expensesQuery = db.collectionGroup('expenses')
    .where('last_email_sent', '<', now - ONE_WEEK).where('state', 'in', ['submitted', 'approved']);
  const expensesResult = await expensesQuery.get();
  expensesResult.forEach((expense) => {
    const school = expense.ref.parent.parent.id;
    if (school !== 'demo') {
      sendEmail(expense.ref, expense.data(), school, true);
    }
  });
});

export const expenseChanged = functions.firestore.document('/schools/{school}/expenses/{expenseNum}').onWrite(async (change, context) => {
  functions.logger.info('expenseChanged');

  if (context.params.school !== 'demo') {
    // Get an object with the current document value.
    // If the document does not exist, it has been deleted.
    const document = change.after.exists ? change.after.data() : null;

    // Get an object with the previous document value (for update or delete)
    const oldDocument = change.before.data();

    // perform desired operations ...
    functions.logger.info('oldDocument', { oldDocument });
    functions.logger.info('document', { document });

    if (document &&
      (oldDocument === undefined ||
        oldDocument.state !== document.state ||
        document.last_email_sent === 0)) {
      // State changed, send email:
      sendEmail(change.after.ref, document, change.after.ref.parent.parent.id, false);
    }
  }
});

async function addUrlToExpense(expense) {
  // Convert receipt file paths into clickable URLs before returning.
  const ONE_HOUR = 1000 * 60 * 60;
  const urlOptions = {
    version: 'v4',
    action: 'read',
    expires: Date.now() + ONE_HOUR
  };
  for (const receipt of expense.receipts) {
    // Since the emulator doesn't support signed URLs, test with a public URL.  See:
    // https://github.com/firebase/firebase-tools/issues/3400
    const url = process.env.FUNCTIONS_EMULATOR
      ? bucket.file(receipt.path).publicUrl()
      : (await bucket.file(receipt.path).getSignedUrl(urlOptions));
    receipt.url = url;
    delete receipt.path;
  }
}

export const getExpenseData = functions
  //  .runWith({
  //    enforceAppCheck: true // Requests without valid App Check tokens will be rejected.
  //  })
  .https.onCall(async (data, context) => {
    if (!context.auth) {
      // Throwing an HttpsError so that the client gets the error details.
      throw new functions.https
        .HttpsError('failed-precondition',
          'The function must only be called while authenticated.');
    }
    const userName = context.auth.token.email;
    const schoolId = data.school;
    const expenseNum = data.expense_num;
    functions.logger.info(`getExpenseData request received(${schoolId}: ${userName}): `, { data, context });

    return await getExpenseDataImpl(userName, schoolId, expenseNum);
  });

async function getExpenseDataImpl(userName, schoolId, expenseNum) {
  const trace = traceStart();
  try {
    let schoolUser = getSchoolUser(schoolId, userName);
    trace.trace('getSchoolUser');
    let user = getUser(userName);
    trace.trace('getUser');
    let expense = getExpense(schoolId, expenseNum);
    trace.trace('getExpense');
    let school = getSchool(schoolId);
    trace.trace('getSchool');
    // Let all three requests run in parallel:
    schoolUser = await schoolUser;
    user = await user;
    expense = await expense;
    school = await school;
    trace.trace('await');

    if (schoolUser && user && expense && school) {
      const isCheckWriter = schoolUser.data().is_check_writer;
      const isSchoolAdmin = schoolUser.data().is_school_admin || schoolId === 'demo';
      const isAuditor = schoolUser.data().is_auditor;
      const isSysAdmin = user.data().is_sysadmin;
      // Verify that the user is allowed to view this expense.
      // Cases when a user is allowed to view an expense:
      //  - user submitted the expense
      //  - user is approver for expense
      //  - user is a check writer
      //  - user is school admin or auditor
      //  - user is global admin
      if (expense.data().submitter === userName ||
        expense.data().approvers.includes(userName) ||
        isCheckWriter ||
        isSchoolAdmin ||
        isAuditor ||
        isSysAdmin) {
        const expenseData = expense.data();
        await addUrlToExpense(expenseData);
        const expenses = [];
        expenses.push(expenseData);
        await attachNamesToEmails(expenses, isCheckWriter ||
          isSchoolAdmin ||
          isAuditor ||
          isSysAdmin, userName);
        trace.trace('reformatExpense');
        expenseData.approvers_needed = await approversNeeded(school.data(), expense.data(), userName);
        trace.trace('approversNeeded');
        return expenseData;
      } else {
        functions.logger.error(`Unable to retrieve expense ${userName}:${schoolId}:${expenseNum} `);
        trace.trace('throw1');
        throw new functions.https
          .HttpsError('invalid-argument', 'Unable to retrieve expense.');
      }
    } else {
      functions.logger.error(`Unable to retrieve expense ${userName}:${schoolId}:${expenseNum} `);
      trace.trace('throw2');
      throw new functions.https
        .HttpsError('invalid-argument', 'Unable to retrieve expense.');
    }
  } finally {
    trace.end();
  }
}

function addNoteToList(expense, user, note) {
  let notes = expense.data().notes;
  if (notes === undefined) {
    notes = [];
  }
  notes.push({
    user,
    note,
    date: Date.now()
  });
  return notes;
}

export const addNote = functions
  //  .runWith({
  //    enforceAppCheck: true // Requests without valid App Check tokens will be rejected.
  //  })
  .https.onCall(async (data, context) => {
    if (!context.auth) {
      // Throwing an HttpsError so that the client gets the error details.
      throw new functions.https.HttpsError('failed-precondition',
        'The function must be called ' +
        'while authenticated.');
    }
    const userName = context.auth.token.email;
    const schoolId = data.school;
    const expenseNum = data.expense_num;
    const newNote = data.note;
    const version = data.version;
    functions.logger.info(`addNote request received(${schoolId}: ${userName}): `, { data, context });

    return await addNoteImpl(userName, schoolId, expenseNum, newNote, version);
  });

async function addNoteImpl(userName, schoolId, expenseNum, newNote, version) {
  try {
    await db.runTransaction(async (transaction) => {
      const expense = await getExpense(schoolId, expenseNum, transaction);

      if (expense.data().version !== version) {
        throw new Error(
          'Another user edited this expense at the same time as you.  Please refresh your browser and try again.');
      }

      let valid = false;
      if (expense && newNote !== undefined && newNote.length <= MAX_STRING_LENGTH) {
        valid = expense.data().approvers.includes(userName) ||
          expense.data().submitter === userName || schoolId === 'demo';
        if (!valid) {
          const schoolUser = await getSchoolUser(schoolId, userName, transaction);

          valid = (schoolUser && (schoolUser.data().is_check_writer ||
            schoolUser.data().is_school_admin));
        }
        if (valid) {
          const notes = addNoteToList(expense, userName, newNote);
          const expenseRef = db.collection('schools').doc(schoolId).collection('expenses').doc(expenseNum);
          const newVersion = version + 1;
          await transaction.update(expenseRef, {
            notes,
            version: newVersion
          });
          return { version: newVersion };
        }
      }
      if (!valid) {
        throw new Error('Invalid argument.');
      }
    });
  } catch (error) {
    functions.logger.error(`${error}`, { error });
    throw new functions.https
      .HttpsError('internal',
        `Unable to write note to database: ${error}`);
  }
}

export const changeBudget = functions
  //  .runWith({
  //    enforceAppCheck: true // Requests without valid App Check tokens will be rejected.
  //  })
  .https.onCall(async (data, context) => {
    if (!context.auth) {
      // Throwing an HttpsError so that the client gets the error details.
      throw new functions.https.HttpsError('failed-precondition',
        'The function must be called ' +
        'while authenticated.');
    }
    const userName = context.auth.token.email;
    const schoolId = data.school;
    const expenseNum = data.expense_num;
    const newBudgetIndex = data.budget;
    const version = data.version;
    functions.logger.info(`changeBudget request received(${schoolId}: ${userName}): `, { data, context });

    return await changeBudgetImpl(userName, schoolId, expenseNum, newBudgetIndex, version);
  });

async function changeBudgetImpl(userName, schoolId, expenseNum, newBudgetIndex, version) {
  try {
    await db.runTransaction(async (transaction) => {
      const school = await getSchool(schoolId, transaction);
      const expense = await getExpense(schoolId, expenseNum, transaction);
      let valid = false;

      if (expense.data().version !== version) {
        throw new Error(
          'Another user edited this expense at the same time as you.  Please refresh your browser and try again.');
      }

      if (school && expense && newBudgetIndex >= 0 && newBudgetIndex < school.data().approvers.length) {
        if (expense.data().approvers.includes(userName)) {
          valid = true;
        } else {
          const schoolUser = await getSchoolUser(schoolId, userName, transaction);
          valid = (schoolUser && schoolUser.data().is_check_writer);
        }
      }
      if (valid) {
        const budget = school.data().approvers[newBudgetIndex].role;
        const approvers = school.data().approvers[newBudgetIndex].ids;
        const notes = addNoteToList(expense, userName,
          `Changed budget from ${expense.data().budget} to ${budget}.`);

        const expenseRef = db.collection('schools').doc(schoolId).collection('expenses').doc(expenseNum);
        await transaction.update(expenseRef, {
          budget,
          approvers,
          notes,
          state: 'submitted',
          last_email_sent: 0, // Trigger new email being sent.
          version: version + 1
        });
      } else {
        throw new Error('Invalid argument.');
      }
    });
  } catch (error) {
    functions.logger.error('Unable to change budget: ', { error });
    throw new functions.https
      .HttpsError('internal',
        'Unable to change budget:' + error);
  }
};

export const approveExpense = functions
  //  .runWith({
  //    enforceAppCheck: true // Requests without valid App Check tokens will be rejected.
  //  })
  .https.onCall(async (data, context) => {
    if (!context.auth) {
      // Throwing an HttpsError so that the client gets the error details.
      throw new functions.https.HttpsError('failed-precondition',
        'The function must be called ' +
        'while authenticated.');
    }
    const userName = context.auth.token.email;
    const schoolId = data.school;
    const expenseNum = data.expense_num;
    const version = data.version;
    functions.logger.info(`approveExpense request received(${schoolId}: ${userName}): `, { data, context });

    return await approveExpenseImpl(userName, schoolId, expenseNum, version);
  });

async function approveExpenseImpl(userName, schoolId, expenseNum, version) {
  try {
    await db.runTransaction(async (transaction) => {
      const expense = await getExpense(schoolId, expenseNum, transaction);

      if (expense.data().version !== version) {
        throw new Error(
          'Another user edited this expense at the same time as you.  Please refresh your browser and try again.');
      }

      let valid = (expense && expense.data().approvers.includes(userName) &&
        (expense.data().state === 'submitted' ||
          expense.data().state === 'rejected'));
      if (!valid) {
        const schoolUser = await getSchoolUser(schoolId, userName, transaction);
        valid = (schoolUser && schoolUser.data().is_check_writer) &&
          (expense && (expense.data().state === 'submitted' ||
            expense.data().state === 'rejected' ||
            expense.data().state === 'approved'));
      }

      if (valid) {
        const notes = addNoteToList(expense, userName, 'Approved expense.');

        // If the user has not previously approved, add them to the approved_by
        // list:
        const approvedBy = (typeof expense.data().approved_by === 'undefined') ? [] : expense.data().approved_by;
        if (!approvedBy.includes(userName)) {
          approvedBy.push(userName);
        }

        const expenseRef = db.collection('schools').doc(schoolId).collection('expenses').doc(expenseNum);
        await transaction.update(expenseRef, {
          state: 'approved',
          notes,
          approved_by: approvedBy,
          version: version + 1
        });
      } else {
        throw new Error('Invalid argument.');
      }
    });
  } catch (error) {
    functions.logger.error('Unable to approve expense: ', { error });
    throw new functions.https
      .HttpsError('internal',
        'Unable to approve expense:' + error);
  }
}

export const rejectExpense = functions
  //  .runWith({
  //    enforceAppCheck: true // Requests without valid App Check tokens will be rejected.
  //  })
  .https.onCall(async (data, context) => {
    if (!context.auth) {
      // Throwing an HttpsError so that the client gets the error details.
      throw new functions.https.HttpsError('failed-precondition',
        'The function must be called ' +
        'while authenticated.');
    }
    const userName = context.auth.token.email;
    const schoolId = data.school;
    const expenseNum = data.expense_num;
    const version = data.version;
    functions.logger.info(`rejectExpense request received(${schoolId}: ${userName}): `, { data, context });

    return await rejectExpenseImpl(userName, schoolId, expenseNum, version);
  });

async function rejectExpenseImpl(userName, schoolId, expenseNum, version) {
  try {
    await db.runTransaction(async (transaction) => {
      const expense = await getExpense(schoolId, expenseNum, transaction);

      if (expense.data().version !== version) {
        throw new Error(
          'Another user edited this expense at the same time as you.  Please refresh your browser and try again.');
      }

      let valid = (expense && expense.data().approvers.includes(userName) &&
        (expense.data().state === 'submitted' ||
          expense.data().state === 'approved'));
      if (!valid) {
        const schoolUser = await getSchoolUser(schoolId, userName, transaction);
        valid = (schoolUser && schoolUser.data().is_check_writer) &&
          (expense && (expense.data().state === 'submitted' ||
            expense.data().state === 'approved'));
      }

      if (valid) {
        const notes = addNoteToList(expense, userName, 'Rejected expense.');

        // If this user previously approved this expense, remove them from the
        // approved_by list:
        const approvedBy = (typeof expense.data().approved_by === 'undefined') ? [] : expense.data().approved_by;
        const index = approvedBy.indexOf(userName);
        if (index !== -1) {
          approvedBy.splice(index, 1);
        }

        const expenseRef = db.collection('schools').doc(schoolId).collection('expenses').doc(expenseNum);
        await transaction.update(expenseRef, {
          state: 'rejected',
          notes,
          approved_by: approvedBy,
          version: version + 1
        });
      } else {
        throw new Error('Invalid argument.');
      }
    });
  } catch (error) {
    functions.logger.error('Unable to reject expense: ', { error });
    throw new functions.https
      .HttpsError('internal',
        'Unable to reject expense:' + error);
  }
}

export const markPaid = functions
  //  .runWith({
  //    enforceAppCheck: true // Requests without valid App Check tokens will be rejected.
  //  })
  .https.onCall(async (data, context) => {
    if (!context.auth) {
      // Throwing an HttpsError so that the client gets the error details.
      throw new functions.https.HttpsError('failed-precondition',
        'The function must be called ' +
        'while authenticated.');
    }
    const userName = context.auth.token.email;
    const schoolId = data.school;
    const expenseNum = data.expense_num;
    const referenceNum = data.reference_num;
    const version = data.version;
    functions.logger.info(`markPaid request received(${schoolId}: ${userName}): `, { data, context });

    return await markPaidImpl(userName, schoolId, expenseNum, referenceNum, version);
  });

// If userName approves this expense, how many more approvals are required
// before we can mark it paid?
async function approversNeeded(school, expense, userName, transaction) {
  let neededApprovers = 1;
  for (const payVia of school.pay_via) {
    if (payVia.type === expense.pay_via) {
      neededApprovers = payVia.approvers;
    }
  }

  const approvers = new Set();
  if (userName) {
    approvers.add(userName);
  }
  const approverList = [];
  if (expense.approved_by !== undefined) {
    for (const approvedBy of expense.approved_by) {
      approverList.push(getSchoolUser(school.id, approvedBy, transaction));
    }
  }
  for (const approver of approverList) {
    const approverUser = await approver;
    if (!approverUser) {
      throw new Error('Error looking up user.');
    }
    if (approverUser.data().is_check_writer) {
      approvers.add(approverUser.data().email);
    }
  }
  return neededApprovers - approvers.size;
}

async function markPaidImpl(userName, schoolId, expenseNum, referenceNum, version) {
  try {
    await db.runTransaction(async (transaction) => {
      let expense = getExpense(schoolId, expenseNum, transaction);
      let schoolUser = getSchoolUser(schoolId, userName, transaction);
      let school = getSchool(schoolId, transaction);

      // Let all three requests run in parallel:
      expense = await expense;
      schoolUser = await schoolUser;
      school = await school;

      if (expense.data().version !== version) {
        throw new Error(
          'Another user edited this expense at the same time as you.  Please refresh your browser and try again.');
      }

      if (!expense || !schoolUser || !school) {
        throw new Error('Invalid argument.');
      }

      const needed = await approversNeeded(school.data(), expense.data(), userName, transaction);
      if (needed > 0) {
        throw new Error(`Can't mark paid, needs approval from ${needed} more check writers.`);
      }

      if (schoolUser.data().is_check_writer &&
        expense.data().state === 'approved') {
        const notes = addNoteToList(expense, userName, 'Marked paid.' +
          ((referenceNum && referenceNum.length > 0)
            ? ` Check/Reference Number: ${referenceNum}`
            : ''));

        const expenseRef = db.collection('schools').doc(schoolId).collection('expenses').doc(expenseNum);
        if (referenceNum && referenceNum.length > 0) {
          await transaction.update(expenseRef, {
            state: 'paid',
            reference_num: referenceNum,
            notes,
            paid_by: userName,
            date_paid: Date.now(),
            version: version + 1
          });
        } else {
          await transaction.update(expenseRef, {
            state: 'paid',
            notes,
            paid_by: userName,
            date_paid: Date.now(),
            version: version + 1
          });
        }
      } else {
        throw new Error('Invalid argument.');
      }
    });
  } catch (error) {
    throw new functions.https
      .HttpsError('internal',
        'Unable to mark paid:' + error);
  }
}

export const getExpenseList = functions
  //  .runWith({
  //    enforceAppCheck: true // Requests without valid App Check tokens will be rejected.
  //  })
  .https.onCall(async (data, context) => {
    if (!context.auth) {
      // Throwing an HttpsError so that the client gets the error details.
      throw new functions.https.HttpsError('failed-precondition',
        'The function must be called ' +
        'while authenticated.');
    }

    const schoolId = data.school;
    const userName = context.auth.token.email;
    const type = data.type;
    functions.logger.info(`getExpenseList request received (${schoolId}: ${userName}):`, { data, context });

    return await getExpenseListImpl(schoolId, userName, type);
  });

async function getExpenseListImpl(schoolId, userName, type) {
  let school = getSchool(schoolId);
  let user = getUser(userName);
  let schoolUser = getSchoolUser(schoolId, userName);

  school = await school;
  user = await user;
  schoolUser = await schoolUser;

  if (school && user && (user.data().is_sysadmin || schoolUser)) {
    const isSysAdmin = user.data().is_sysadmin;
    // The sysAdmin is allowed to view a school even if they are not a user at this school:
    const isCheckWriter = isSysAdmin || schoolUser.data().is_check_writer;
    const isSchoolAdmin = isSysAdmin || schoolUser.data().is_school_admin || schoolId === 'demo';
    const isAuditor = isSysAdmin || schoolUser.data().is_auditor;

    const expenses = [];

    switch (type) {
      case 'all': {
        // console.log(`isCheckWriter=${isCheckWriter} isSchoolAdmin=${isSchoolAdmin} isSysAdmin=${isSysAdmin}`);
        if (isCheckWriter || isSchoolAdmin || isAuditor || isSysAdmin) {
          // Just get all of the expenses:
          const allExpenses = await db.collection('schools').doc(schoolId).collection('expenses').get();
          allExpenses.forEach((expense) => {
            expenses.push(expense.data());
          });
          // console.log(expenses);
        }
        break;
      }
      case 'submitter': {
        const myExpensesPromise = db.collection('schools').doc(schoolId).collection('expenses')
          .where('submitter', '==', userName).get();
        const myExpenses = await myExpensesPromise;
        myExpenses.forEach((expense) => {
          expenses.push(expense.data());
        });
        break;
      }
      case 'approver': {
        const iAmApproverPromise = db.collection('schools').doc(schoolId).collection('expenses')
          .where('approvers', 'array-contains', userName).get();
        const iAmApprover = await iAmApproverPromise;
        iAmApprover.forEach((expense) => {
          expenses.push(expense.data());
        });
        break;
      }
    }

    await attachNamesToEmails(expenses, isCheckWriter ||
      isSchoolAdmin ||
      isAuditor ||
      isSysAdmin, userName);

    // console.log('Expenses:', expenses);
    return { expenses };
  } else {
    throw new functions.https.HttpsError('not-found', 'Invalid argument.');
  }
}

async function attachNamesToEmails(expenses, keepNames, userName) {
  for (const expense of expenses) {
    if (expense.approvers.includes(userName)) {
      expense.is_approver = true;
    }
  }

  if (!keepNames) {
    // If the user is not in some official role, they could be anyone from the
    // internet.  Don't share names and emails with them, just strip them out:
    for (const expense of expenses) {
      if (expense.approved_by) {
        delete expense.approved_by;
      }
      delete expense.approvers;
      expense.submitter_name = (await getUser(expense.submitter)).data().name || '';
      for (const note of expense.notes) {
        delete note.user;
      }
    }
  } else {
    // Since looking up names from emails is slow, first we gather all the
    // emails from our list of expenses, then we look them up once, then we attach
    // them to the expenses.
    const emails = new Map();
    for (const expense of expenses) {
      emails.set(expense.submitter, {});
      if (expense.approved_by && typeof expense.approved_by !== 'string') {
        for (const approvedBy of expense.approved_by) {
          emails.set(approvedBy, {});
        }
      }
      for (const approver of expense.approvers) {
        emails.set(approver, {});
      }
    }
    // Launch requests and get promises:
    for (const email of emails) {
      emails.set(email[0], getUser(email[0]));
    }
    // Wait for promises to resolve:
    for (const email of emails) {
      emails.set(email[0], await email[1]);
    }

    function getName(email, emails) {
      const result = emails.get(email);
      if (result) {
        const data = result.data();
        if (data) {
          const name = data.name;
          if (name) {
            return name;
          }
        }
      }
      return '';
    }

    for (const expense of expenses) {
      expense.submitter_name = getName(expense.submitter, emails);

      if (expense.approved_by) {
        const approvedBy = expense.approved_by;
        delete expense.approved_by;
        expense.approved_by = [];
        for (const approvedByEmail of approvedBy) {
          const approvedByName = getName(approvedByEmail, emails);
          expense.approved_by.push({
            email: approvedByEmail,
            name: approvedByName
          });
        }
      }
      const approvers = expense.approvers;
      expense.approvers = [];
      for (const approver of approvers) {
        const approverName = getName(approver, emails);
        expense.approvers.push({
          email: approver,
          name: approverName
        });
      }
    }
  }
}

export const addSchool = functions
  //  .runWith({
  //    enforceAppCheck: true // Requests without valid App Check tokens will be rejected.
  //  })
  .https.onCall(async (data, context) => {
    if (!context.auth) {
      // Throwing an HttpsError so that the client gets the error details.
      throw new functions.https
        .HttpsError('failed-precondition',
          'The function must only be called while authenticated.');
    }
    const userName = context.auth.token.email;
    const schoolId = data.school;
    const schoolName = data.name;
    functions.logger.info(`addSchool request received (${schoolId}: ${userName}):`, { data, context });

    const user = await getUser(userName);

    if (!user || !user.data().is_sysadmin) {
      throw new functions.https.HttpsError('failed-precondition',
        'Permission denied.');
    }

    return await addSchoolImpl(schoolId, schoolName);
  });

async function addSchoolImpl(schoolId, schoolName) {
  if ((await getSchool(schoolId)) !== null) {
    throw new functions.https.HttpsError('failed-precondition',
      'School already exists.');
  }
  await db.collection('schools').doc(schoolId).set({
    id: schoolId,
    name: schoolName,
    last_expense_num: 0,
    pay_via: [],
    approvers: []
  });
}

export const getSchoolData = functions
  //  .runWith({
  //    enforceAppCheck: true // Requests without valid App Check tokens will be rejected.
  //  })
  .https.onCall(async (data, context) => {
    if (!context.auth) {
      // Throwing an HttpsError so that the client gets the error details.
      throw new functions.https
        .HttpsError('failed-precondition',
          'The function must only be called while authenticated.');
    }
    const userName = context.auth.token.email;
    const schoolId = data.school;
    functions.logger.info(`getSchoolData request received (${schoolId}: ${userName}):`, { data, context });
    const school = getSchool(schoolId);
    const schoolUser = await getSchoolUser(schoolId, userName);

    if ((!schoolUser || !schoolUser.data().is_school_admin) && schoolId !== 'demo') {
      const user = await getUser(userName);

      if (!user || !user.data().is_sysadmin) {
        throw new functions.https.HttpsError('failed-precondition',
          'Permission denied.');
      }
    }
    if (!(await school)) {
      throw new functions.https.HttpsError('failed-precondition',
        'Invalid school.');
    }

    return await getSchoolDataImpl(schoolId);
  });

async function getSchoolDataImpl(schoolId) {
  const school = await getSchool(schoolId);
  if (!(await school)) {
    throw new functions.https.HttpsError('failed-precondition',
      'Invalid school.');
  }

  const schoolAdmins = [];
  const checkWriters = [];
  const auditors = [];
  const allUsers = await db.collection('schools').doc(schoolId).collection('school_users').get();
  allUsers.forEach((schoolUser) => {
    const email = schoolUser.data().email;
    if (schoolUser.data().is_school_admin) {
      schoolAdmins.push(email);
    }
    if (schoolUser.data().is_check_writer) {
      checkWriters.push(email);
    }
    if (schoolUser.data().is_auditor) {
      auditors.push(email);
    }
  });
  return {
    school_id: school.data().id,
    school_name: school.data().name,
    approvers: school.data().approvers,
    pay_via: school.data().pay_via,
    school_admins: schoolAdmins,
    check_writers: checkWriters,
    auditors
  };
}

export const updateSchool = functions
  //  .runWith({
  //    enforceAppCheck: true // Requests without valid App Check tokens will be rejected.
  //  })
  .https.onCall(async (data, context) => {
    if (!context.auth) {
      // Throwing an HttpsError so that the client gets the error details.
      throw new functions.https
        .HttpsError('failed-precondition',
          'The function must only be called while authenticated.');
    }
    // FIXME:  should verify the types of every argument, so someone doesn't pass
    // in a random structure as approvers or the like.
    const userName = context.auth.token.email;
    const schoolId = data.school;
    functions.logger.info(`updateSchool request received (${schoolId}: ${userName}):`, { data, context });
    const schoolName = data.school_name;
    const approvers = data.approvers;
    const payVia = data.pay_via;
    const schoolAdmins = data.school_admins;
    const checkWriters = data.check_writers;
    const auditors = data.auditors;
    const school = getSchool(schoolId);
    const schoolUser = await getSchoolUser(schoolId, userName);

    if ((!schoolUser || !schoolUser.data().is_school_admin) && schoolId !== 'demo') {
      const user = await getUser(userName);

      if (!user || !user.data().is_sysadmin) {
        throw new functions.https.HttpsError('failed-precondition',
          'Permission denied.');
      }
    }
    if (!(await school)) {
      throw new functions.https.HttpsError('failed-precondition',
        'Invalid school.');
    }

    return await updateSchoolImpl(schoolId, schoolName, approvers, payVia, schoolAdmins, checkWriters, auditors);
  });

async function updateSchoolImpl(schoolId, schoolName, approvers, payVia, schoolAdmins, checkWriters, auditors) {
  try {
    await db.runTransaction(async (transaction) => {
      const schoolUsers = new Map();

      // Get a full list of all the existing school users and their permissions:
      const allSchoolUsers = await transaction.get(db.collection('schools').doc(schoolId).collection('school_users'));
      allSchoolUsers.forEach((schoolUser) => {
        schoolUsers.set(schoolUser.id, {
          is_school_admin: schoolUser.data().is_school_admin,
          is_check_writer: schoolUser.data().is_check_writer,
          is_auditor: schoolUser.data().is_auditor
        });
      });

      const schoolUsersModified = new Set();

      // If our list of approvers contains an email not in our system, we need to add it:
      for (const approver of approvers) {
        for (const id of approver.ids) {
          if (schoolUsers.get(id)) {
            // NOP, user exists.
          } else {
            schoolUsers.set(id, {
              is_school_admin: false,
              is_check_writer: false,
              is_auditor: false
            });
            schoolUsersModified.add(id);
          }
        }
      }

      // Is the school admins, check writers, or auditors contains an email not
      // in our system, we need to add it.  We'll update their permissions
      // shortly:
      for (const email of schoolAdmins.concat(checkWriters, auditors)) {
        const su = schoolUsers.get(email);
        if (!su) {
          schoolUsers.set(email, {
            is_school_admin: false,
            is_check_writer: false,
            is_auditor: false
          });
          schoolUsersModified.add(email);
        }
      }

      // Iterate through *all* users and update their permissions:
      for (const [email, perms] of schoolUsers) {
        const isSchoolAdmin = schoolAdmins.includes(email);
        const isCheckWriter = checkWriters.includes(email);
        const isAuditor = auditors.includes(email);

        if (isSchoolAdmin !== perms.is_school_admin) {
          perms.is_school_admin = isSchoolAdmin;
          schoolUsersModified.add(email);
        }
        if (isCheckWriter !== perms.is_check_writer) {
          perms.is_check_writer = isCheckWriter;
          schoolUsersModified.add(email);
        }
        if (isAuditor !== perms.is_auditor) {
          perms.is_auditor = isAuditor;
          schoolUsersModified.add(email);
        }
      }

      // Now that we know exactly what users need to be updated, go through and
      // update them all.  Do all the updates in parallel, and we'll wait for
      // them to complete later:
      const updates = [];
      for (const email of schoolUsersModified) {
        const schoolUserRef = db.collection('schools').doc(schoolId).collection('school_users').doc(email);
        updates.push(transaction.set(schoolUserRef, schoolUsers.get(email)));
      }

      // Update the school itself too:
      const schoolUpdate = {
        approvers,
        pay_via: payVia
      };
      if (schoolName) {
        schoolUpdate.name = schoolName;
      }
      updates.push(transaction.update(db.collection('schools').doc(schoolId), schoolUpdate));

      // Now we wait for all of our updates to complete:
      Promise.all(updates);
    });
  } catch (error) {
    functions.logger.error('Unable to update school: ', { error });
    throw new functions.https
      .HttpsError('internal',
        'Unable to update school:' + error);
  }
}

// FIXME:  when a new user is added, check for similarity with prior users, and
// email a warning to check writers if similarity is detected.

export const eraseAllDemoData = functions
  //  .runWith({
  //    enforceAppCheck: true // Requests without valid App Check tokens will be rejected.
  //  })
  .https.onCall(async (data, context) => {
    if (!context.auth) {
      // Throwing an HttpsError so that the client gets the error details.
      throw new functions.https
        .HttpsError('failed-precondition',
          'The function must only be called while authenticated.');
    }
    const userName = context.auth.token.email;
    functions.logger.info(`eraseAllDemoData request received (${userName}):`, { data, context });

    return await eraseAllDemoDataImpl();
  });

async function eraseAllDemoDataImpl() {
  const allExpenses = await db.collection('schools').doc('demo').collection('expenses').get();
  const promises = [];
  allExpenses.forEach((expense) => {
    promises.push(expense.ref.delete());
  });
  Promise.all(promises);
}

export const generateTestDemoData = functions
  //  .runWith({
  //    enforceAppCheck: true // Requests without valid App Check tokens will be rejected.
  //  })
  .https.onCall(async (data, context) => {
    if (!context.auth) {
      // Throwing an HttpsError so that the client gets the error details.
      throw new functions.https
        .HttpsError('failed-precondition',
          'The function must only be called while authenticated.');
    }
    const email = context.auth.token.email;
    const uid = context.auth.uid;
    const name = context.auth.token.name;
    functions.logger.info(`generateTestDemoData request received (${email}):`, { data, context });

    const people = [
      { // 0
        name,
        email,
        uid,
        school: 'demo'
      },
      { // 1
        name: 'Joan',
        email: 'joan_test@parentpayback.com',
        uid: 123456789,
        school: 'demo'
      },
      { // 2
        name: 'Katie',
        email: 'katie_test@parentpayback.com',
        uid: 123456789,
        school: 'demo'
      },
      { // 3
        name: 'Karina',
        email: 'karina_test@parentpayback.com',
        uid: 123456789,
        school: 'demo'
      },
      { // 4
        name: 'Joyce',
        email: 'joyce_test@parentpayback.com',
        uid: 123456789,
        school: 'demo'
      },
      { // 5
        name: 'Linda',
        email: 'linda_test@parentpayback.com',
        uid: 123456789,
        school: 'demo'
      },
      { // 6
        name: 'Erin',
        email: 'erin_test@parentpayback.com',
        uid: 123456789,
        school: 'demo'
      },
      { // 7
        name: 'Lindsay',
        email: 'lindsay_test@parentpayback.com',
        uid: 123456789,
        school: 'demo'
      },
      { // 8
        name: 'Barry',
        email: 'barry_test@parentpayback.com',
        uid: 123456789,
        school: 'demo'
      },
      { // 9
        name: 'Yvette',
        email: 'yvette_test@parentpayback.com',
        uid: 123456789,
        school: 'demo'
      },
      { // 10
        name: 'Jane',
        email: 'jane_test@parentpayback.com',
        uid: 123456789,
        school: 'demo'
      },
      { // 11
        name: 'Leslie',
        email: 'leslie_test@parentpayback.com',
        uid: 123456789,
        school: 'demo'
      },
      { // 12
        name: 'Demo Treasurer',
        email: 'demo_treasurer@parentpayback.com',
        uid: 123456789,
        school: 'demo'
      },
      { // 13
        name: 'Demo Admin',
        email: 'demo_admin@parentpayback.com',
        uid: 123456789,
        school: 'demo'
      },
      { // 14
        name: 'Chris',
        email: 'chris@parentpayback.com',
        uid: 123456789,
        school: 'demo'
      }
    ];

    // console.log('updateSchoolImpl:');
    await updateSchoolImpl('demo', 'Demo Elementary',
      [
        { role: 'President', name: people[0].name + '/' + people[7].name, ids: [people[0].email, people[7].email] },
        { role: 'Ways and Means', name: people[2].name, ids: [people[2].email] },
        { role: 'Membership and Outreach', name: people[3].name + '/' + people[4].name, ids: [people[3].email, people[4].email] },
        { role: 'Events and Appreciation', name: people[5].name + '/' + people[6].name, ids: [people[5].email, people[6].email] }
      ],
      [
        { type: 'Check', approvers: 1 },
        { type: 'PayPal', approvers: 2 }
      ],
      [people[0].email, people[13].email], [people[12].email, people[0].email], []);

    // console.log('getUserDataImpl:');
    for (const p of people) {
      await getUserDataImpl(p.uid, p.email, p.name, p.school);
    }
    const purposes = [
      'Donuts for teacher appreciation',
      'Pencils and paper',
      'Insurance fees',
      'Little monkey cleanup',
      'Tissues for grownups',
      'Donkey food for ride',
      'Gloves'
    ];
    const payees = [
      {
        name: 'Please McPayMe',
        street: '14 Main St.',
        city: 'Palo Alto',
        state: 'CA',
        zip: '94301',
        email: 'mcpayme@parentpayback.com',
        phone: '555-555-1212'
      },
      {
        name: 'Gimme Cash',
        street: '22 Main St.',
        city: 'Palo Alto',
        state: 'CA',
        zip: '94301',
        email: 'cash@parentpayback.com',
        phone: '555-555-1213'
      },
      {
        name: 'Justa Name',
        street: '',
        city: '',
        state: '',
        zip: '',
        email: '',
        phone: ''
      }
    ];

    const notes = [
      'Is this thing on?',
      'I just called, to say, I love you.  I just called, to say how much I care.',
      '',
      '',
      '',
      '',
      '',
      '',
      '',
      '',
      ''
    ];

    const filenames = [
      'foo.pdf',
      'receipt.gif',
      'teachertreats.pdf',
      'averylongfilenamethatshouldoverflowthisbox.pdf'
    ];
    const itemDescriptions = [
      'A box of food.',
      'Too many donuts.',
      'Anything but a microwave for the lounge.',
      'Stickers.  Loads of stickers.',
      'Tent poles for mysterious tents.'
    ];

    function randomDollar(limit) {
      return (Math.floor(Math.random() * limit * 100) / 100).toFixed(2);
    }

    // console.log('submitExpenseImpl:');
    for (let i = 0; i < 30; i++) {
      await submitExpenseImpl(people[i % 4].email,
        people[i % 4].school,
        purposes[i % purposes.length],
        (i % 4).toString(),
        payees[i % payees.length].name,
        payees[i % payees.length].street,
        payees[i % payees.length].city,
        payees[i % payees.length].state,
        payees[i % payees.length].zip,
        payees[i % payees.length].email,
        payees[i % payees.length].phone,
        'Check',
        false,
        randomDollar(5), randomDollar(5),
        [{
          name: filenames[i % filenames.length],
          amount: randomDollar(100),
          description: itemDescriptions[i % itemDescriptions.length],
          path: 'demo/fake_receipt.jpg'
        }],
        notes[i % notes.length]);
    }
    // console.log('Generate test data DONE');
    return {};
  });

function traceStart() {
  return {
    data: [{
      message: 'START',
      time: Date.now()
    }],
    trace: function (message) {
      this.data.push({
        message,
        time: Date.now()
      });
    },
    end: function () {
      this.trace('END');
      const start = this.data[0].time;
      let last = start;
      let message = 'START 0ms';
      for (let i = 0; i < this.data.length; i++) {
        const cur = this.data[i].time;
        message += ' | ' + this.data[i].message + ' ' + (cur - last) + 'ms';
        last = cur;
      }
      message += ' | TOTAL: ' + (last - start) + 'ms';
      functions.logger.info('trace:', { message });
    }
  };
}
