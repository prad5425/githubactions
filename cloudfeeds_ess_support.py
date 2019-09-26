#!/usr/local/bin/python2.7

# Standard imports
import logging
import time

# Application imports
import clients
import CORE
import CTK
from CTK.QueueView import QvQueueView
from CTK.QueueView import QvQueueViewCondition
from CTK.QueueView import QvQueueViewConditionField
from CTK.QueueView import QvOperator
from CTK.QueueView import QvCondition
import json_lib


MARKER_FILE_PATH = "/home/core/var/run/cloudfeeds_ess_support_marker.txt"
SLEEP_INTERVAL = 5

LOGGER = logging.getLogger("daemons.cloudfeeds_ess_support")

Role = CTK.Account.Role
role_map = {
    "ACCOUNT_COORDINATOR": Role.ACCOUNT_COORDINATOR,
    "ACCOUNT_MANAGER": Role.ACCOUNT_EXECUTIVE,
    "ACCOUNTS_RECEIVABLE_SPECIALIST": Role.ACCOUNTS_RECEIVABLE,
    "BUSINESS_DEVELOPMENT_CONSULTANT": Role.BUSINESS_DEVELOPMENT,
    "CROSS_PLATFORM_LEAD_TECH": Role.CROSS_PLATFORM_TECH_LEAD,
    "INTERNAL_REVIEWER": Role.EMPLOYEE_REVIEWER,
    "PRIMARY_LEAD_TECH": Role.PRIMARY_LEAD_TECH,
    "SYSTEM_ADMINISTRATOR": Role.SYSTEM_ADMINISTRATOR
}

ctk_attribute_role_map = {
        "ACCOUNT_COORDINATOR": "account_coordinator",
        "ACCOUNT_MANAGER": "account_exec",
        "ACCOUNTS_RECEIVABLE_SPECIALIST": "ar_specialist",
        "BUSINESS_DEVELOPMENT_CONSULTANT": "business_development",
        "CROSS_PLATFORM_LEAD_TECH": "cross_platform_lead_techs",
        "INTERNAL_REVIEWER": "internal_reviewers",
        "PRIMARY_LEAD_TECH": "primary_lead_tech",
        "SYSTEM_ADMINISTRATOR": "system_administrator"
    }


def remove_account_roles(account, roles):
    for key in ctk_attribute_role_map:
        old_contacts = []
        contact_role = getattr(account, ctk_attribute_role_map[key])

        if isinstance(contact_role, list):
            old_contacts.extend(contact_role)
        elif contact_role:
            old_contacts.append(contact_role)

        for contact in old_contacts:
            if is_role_deleted(roles, key, contact.employee_userid):
                acct_contact_role = CORE.ACCT.AccountContacts.loadList(
                    account=account.id,
                    contact=contact.id,
                    role=role_map[key]
                )

                if acct_contact_role:
                    acct_contact_role[0].delete()

                    account.addContactChangeLog(contact, None, role_map[key])


def is_role_deleted(roles, role_name, sso):
    for role in roles:
        if (role.get("role") == role_name and role.get("sso") == sso):
            return False

    return True


def process_account_role_event(account, sso, role_name):
    """
    Creates/adds an Account Contact Role entry in CORE DB

    :param account_number: CORE account number
    :type account_number: str
    :param sso: User SSO of whom the role will be assigned to.
    :type sso: str
    :param role_name: The name of the assigned role. Name must be a key
        in `role_map` dict for an entry to be created.
    :type role_name: str
    :returns: Nothing
    :return: None
    """

    multi_contact_roles = {
        "CROSS_PLATFORM_LEAD_TECH",
        "INTERNAL_REVIEWER",
        "SYSTEM_ADMINISTRATOR"
    }

    if role_name not in role_map:
        LOGGER.info(
            "Role '{}' not found in mapping.  Skipping role.".format(role_name)
        )
        return

    role_id = role_map[role_name]

    contact_where = CTK.Contact.ContactWhere("employee_userid", "=", sso)
    contact = CTK.Contact.Contact.loadList(contact_where)
    if not contact:
        message = "Could not find Contact with matching SSO: {}".format(sso)
        raise CTK.Exceptions.NotFound(message)

    acct_contact_role = CORE.ACCT.AccountContacts.loadList(
            account=account.id, contact=contact[0].id, role=role_id
        )

    if not acct_contact_role:
        if role_name in multi_contact_roles:
            CORE.ACCT.AccountContacts.new(
                account=account.id, contact=contact[0].id, role=role_id
            )
            old_contact = None
        else:
            old_contact = getattr(account, ctk_attribute_role_map[role_name])

            CORE.ACCT.AccountContacts.addAccountContactRole(
                account_id=account.id, contact=contact[0].id, role=role_id
            )

        account.addContactChangeLog(old_contact, contact[0].id, role_id)


def process_account_team_event(account_number, team_name, team_type):
    """
    Assigns a Support Team to an Account.

    :param account_number: CORE account number
    :type account_number: str
    :param team_name: ESS team name
    :type team_name: str
    :param team_type: Team type (i.e. SUPPORT)
    :type team_type: str
    :return: None
    :returns: Nothing
    """
    if not team_type or team_type.lower() != "support":
        LOGGER.debug("Event is not of team type 'Support', skipping.")
        return

    account = CTK.Account.Account.load(int(account_number))
    support_team = CTK.Account.Team.loadList(name=team_name)

    if not support_team:
        message = "Could not find Team with matching name: {}"
        raise CTK.Exceptions.NotFound(message.format(team_name))

    # should be updated once CTK.Account.Account.[gs]etSupportTeam()
    # and CTK.Account.Account.[gs]etType() stop pointing to CRM/CMS
    account._layer1_object.support_team = support_team[0].id
    try:
        team_segment_id = support_team[0]._layer1_object.segment.id
    except AttributeError:
        # assigning Managed segment as a default
        team_segment_id = CTK.Account.Type.MANAGED
    account._layer1_object.segment = team_segment_id


def process_team_event(team_number):
    """
    Creates or updates a team (ACCT_Team)

    :param team_name: Name of the Team to create or update
    :type team_name: str
    :param team_number: Unique ESS identifier of the Team
    :type team_number: str
    :return: None
    :returns: Nothing
    """
    response = clients.ess.ESS.get_team(team_number)

    team_name = response.get("name")
    core_segment = response.get("core_segment")
    region = response.get("region")
    description = response.get("description")

    segments = CTK.Account.Type.loadList(name=core_segment)
    if not segments:
        message = "Could not find Type (segment) with matching name: {}"
        LOGGER.error(message.format(core_segment))
        raise CTK.Exceptions.NotFound(message.format(core_segment))
    segment = segments[0]

    try:
        territory = CTK.Account.SupportTerritory.loadByCode(region)
    except CTK.Exceptions.NotFound:
        message = "Could not find support territory with region code: {}"
        LOGGER.error(message.format(region))
        raise CTK.Exceptions.NotFound(message.format(region))

    teams = CTK.Account.Team.loadList(ess_number=team_number)

    if teams:
        team = teams[0]
        team.name = team_name
        team.segment = segment.id
        team.support_territory = territory.id
        team.description = description
    else:
        team_role_id = CTK.Account.TeamRole.SUPPORT
        crm_team_id = CORE.ACCT.Team.getCRMTeamID()

        team = CORE.ACCT.Team.new(
            name=team_name,
            segment=segment.id,
            role=team_role_id,
            crm_team_id=crm_team_id,
            ess_number=team_number,
            support_territory=territory.id,
            description=description
        )

        team = CTK.Account.Team(team)

        qview = QvQueueView.new(
            label="t%s" % team.id,
            name=team_name,
            description="%s Support Team" % team_name
        )
        qc1 = QvQueueViewCondition.new(
            qview, QvCondition.loadByLabel("team"), 1)
        QvQueueViewConditionField.new(
            qc1, team.id, QvOperator.loadByName("="))
        QvQueueViewCondition.new(
            qview, QvCondition.loadByLabel("or"), 2)
        qc3 = QvQueueViewCondition.new(
            qview, QvCondition.loadByLabel("account"), 3)
        QvQueueViewConditionField.new(
            qc3, "None", QvOperator.loadByName("="))
        QvQueueViewCondition.new(
            qview, QvCondition.loadByLabel("and"), 4)
        qc5 = QvQueueViewCondition.new(
            qview, QvCondition.loadByLabel("queue"), 5)
        QvQueueViewConditionField.new(
            qc5, getSupportQueue(team.segment), QvOperator.loadByName("="))
        qview.permission_group = 1


def getSupportQueue(segment):
    if segment.id == CTK.Account.Type.MANAGED:
        return CTK.Ticket.Queue.Queue.MANAGED
    elif segment.id == CTK.Account.Type.INTENSIVE:
        return CTK.Ticket.Queue.Queue.INTENSIVE
    elif segment.id == CTK.Account.Type.MANAGED_COLOCATION:
        return CTK.Ticket.Queue.Queue.MANAGED_COLOCATION
    elif segment.id == CTK.Account.Type.RACKSPACE_CLOUD:
        return CTK.Ticket.Queue.Queue.CLOUD_DEPLOYMENT
    elif segment in CTK.Account.Type.ENTERPRISE_SERVICES_GROUP:
        return CTK.Ticket.Queue.Queue.ENTERPRISE_SERVICES
    else:
        message = "Segment '{}' (id={}) not found in queue mapping"
        LOGGER.error(message.format(segment, segment.id))
        return None


def get_marker():
    """Reads an UUID from a file to use as a marker"""
    try:
        with open(MARKER_FILE_PATH, "r") as marker_file:
            return marker_file.readline()
    except IOError:
        return ""


def set_marker(marker):
    """Writes a UUID marker into a file"""
    try:
        with open(MARKER_FILE_PATH, "w") as marker_file:
            marker_file.write(marker)
    except IOError:
        LOGGER.debug("Failed to write into '{}'".format(MARKER_FILE_PATH))


def main():
    LOGGER.info("Started")

    CTK.Auth.Auth.authAsSystemAdmin()

    feature_flag = CTK.FeatureFlag.FeatureFlag.FeatureFlag
    can_assign_roles = feature_flag.is_feature_enabled("can_assign_roles")

    categories = {
        # "category": "alias"
        "support.roles.account_support.update.hybrid": "account_role",
        "support.roles.account_support.create.hybrid": "account_role",
        "support.teams.account_support.update.hybrid": "account_team",
        "support.teams.account_support.create.hybrid": "account_team",
        "support.team.team.update": "team",
        "support.team.team.create": "team"
    }

    cats = ["(cat=type:{})".format(term) for term in categories.keys()]
    search_param = "(OR{})".format("".join(cats))

    last_uuid = get_marker()

    try:
        while True:
            response = clients.cloudfeeds.CF.fetch_support(
                params={"marker": last_uuid, "search": search_param}
            )
            feed = json_lib.log_loads(response.text)
            entries = feed.get("feed", {}).get("entry", [])

            if not entries:
                message = "Waiting {} seconds before next fetch attempt"
                LOGGER.debug(message.format(SLEEP_INTERVAL))
                time.sleep(SLEEP_INTERVAL)
                continue

            while entries:
                entry = entries.pop()
                event = entry.get("content", {}).get("event", {})
                product = event.get("product", {})

                account_number = event.get("resourceId")
                if account_number.startswith("hybrid:"):
                    account_number = account_number.split(":")[-1]

                category = None
                for category_term in entry.get("category", []):
                    term = category_term.get("term")
                    if term.startswith("type:"):
                        category = term.split(":")[-1]

                LOGGER.debug("Fetched event: '{}'".format(event))

                try:
                    if categories[category] == "account_role":
                        roles = product.get("role", {})
                        if isinstance(roles, dict):
                            roles = [roles]

                        account = CTK.Account.Account.load(int(account_number))

                        if not can_assign_roles:
                            try:
                                remove_account_roles(account, roles)
                            except Exception as error:
                                LOGGER.error(
                                    "Error removing roles {} ".format(roles)
                                    + "for account {}: ".format(account_number)
                                    + " {}".format(error)
                                )

                        for role in roles:
                            role_name = role.get("role")
                            sso = role.get("sso")

                            try:
                                process_account_role_event(
                                    account=account,
                                    sso=sso,
                                    role_name=role_name
                                )
                            except Exception as error:
                                LOGGER.error(
                                    "Error processing role {} ".format(
                                        role_name
                                    )
                                    + "for account {} with sso {}: ".format(
                                        account_number, sso
                                    )
                                    + "{}".format(error)
                                )

                    elif categories[category] == "account_team":
                        teams = product.get("team", {})
                        if isinstance(teams, dict):
                            teams = [teams]

                        for team in teams:
                            team_name = team.get("teamName")
                            team_type = team.get("teamType")

                            process_account_team_event(
                                account_number=account_number,
                                team_name=team_name,
                                team_type=team_type
                            )

                    elif categories[category] == "team":
                        team_number = product.get("teamNumber")
                        
                        process_team_event(team_number=team_number)

                    CTK.commit()

                except Exception as error:
                    CTK.rollback()
                    message = "An error occurred processing event ({}). {}"
                    LOGGER.info(message.format(entry.get("id"), error))
                    LOGGER.debug(event)

                last_uuid = entry.get("id")

            set_marker(last_uuid)

    except Exception:
        LOGGER.exception("Died")
    finally:
        set_marker(last_uuid)

    LOGGER.info("Stopped")


if __name__ == '__main__':
    main()
