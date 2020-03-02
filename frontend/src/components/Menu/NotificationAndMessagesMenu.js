import React from "react";

import {
    Position,
    Menu,
    MenuItem,
    MenuDivider,
    Popover,
    Icon, Button, Colors, Intent
} from '@blueprintjs/core';

const NotificationAndMessagesMenuItems = () => {

    return (
        <Menu>
            <MenuItem text="Inbox" icon="envelope" />
            <MenuItem text="Chats" icon="chat" />
            <MenuItem text="Subscriber Feeds" icon="feed" />
        </Menu>
    );
};

const NotificationAndMessagesMenu = (props) => {

    return (
        <Popover content={<NotificationAndMessagesMenuItems />} position={Position.BOTTOM_LEFT}>
            <Button style={styles.iconStyles}>
                <Icon style={styles.innstyle} icon="notifications" color={Colors.BLACK} />
            </Button>
        </Popover>
    );

};

const styles = {

    iconStyles : {
        margin: '2px'
    },
    innstyle: {
        margin: '2px'
    }
}

export default NotificationAndMessagesMenu;